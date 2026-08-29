package main

// Koruma katmani.
//
// Faz 12 sistemi dayanikli, Faz 13 isletilebilir yapti. Bu dosya eksik kalan
// ucuncu boyutu kapatir: sistem HALKA ACIK bir adreste durdugunda kotu niyetli
// ya da sadece dikkatsiz bir istemci tarafindan devrilmemesi.
//
// Buradaki uc mekanizma ayni soruya farkli cevaplar veriyor:
//
//	ipRateLimiter    -> "tek bir kaynak ne kadar sik isteyebilir"
//	inFlightLimiter  -> "hepsi birden ne kadar is yaptirabilir"
//	clientIP         -> "kaynak" derken kimi kastettigimiz
//
// Ucuncusu olmadan ilk ikisi ise yaramaz: reverse proxy arkasinda her istek
// ayni IP'den geliyormus gibi gorunur.

import (
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricRejectedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "pulsecity_webviz_rejected_requests_total",
		Help: "Koruma katmani tarafindan reddedilen API istegi sayisi",
	}, []string{"reason"})

	metricInFlight = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "pulsecity_webviz_history_in_flight",
		Help: "Su an islenmekte olan gecmis sorgusu sayisi",
	})

	metricRejectedClients = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pulsecity_webviz_rejected_clients_total",
		Help: "Baglanti tavani doldugu icin reddedilen WebSocket istemcisi sayisi",
	})
)

// --- Kaynak adresi ----------------------------------------------------------

// clientIP, istegin gercek kaynak adresini dondurur.
//
// NEDEN r.RemoteAddr YETMIYOR: prod'da webviz'in portu disa acilmaz
// (deploy/docker-compose.prod.yml -> `ports: []`), butun trafik Caddy uzerinden
// gelir. Yani r.RemoteAddr HER ZAMAN Caddy'nin konteyner IP'sidir. Onu hiz
// siniri anahtari yapmak butun ziyaretcileri tek bir kovaya doldurur: ilk
// kullanici kotayi bitirir, geri kalan herkes 429 yer. Sinir "koruma" degil,
// "kendi kendini engelleme" olurdu.
//
// NEDEN EN SAGDAKI XFF GIRDISI: Caddy (ve uyumlu proxy'ler) X-Forwarded-For'a
// kendi gordugu peer adresini EKLER, basligi sifirdan yazmaz. Istemci
// "X-Forwarded-For: 1.2.3.4" gonderirse baslik "1.2.3.4, <gercek-ip>" olur.
// En SOLDAKI istemcinin uydurdugu deger, en SAGDAKI proxy'nin kendi gordugu
// degerdir. En soldakine guvenen bir hiz siniri, her istekte farkli bir sahte
// IP gondererek tamamen atlatilir - yani hic olmamasindan farksizdir.
//
// Bu, webviz'e YALNIZCA proxy uzerinden ulasilabildigi icin guvenli. Portu
// dogrudan disa acan bir kurulumda (local gelistirme) XFF zaten gelmez ve
// RemoteAddr'a dusulur.
func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if i := strings.LastIndexByte(xff, ','); i >= 0 {
			xff = xff[i+1:]
		}
		if ip := strings.TrimSpace(xff); ip != "" {
			return ip
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// --- IP basina hiz siniri ---------------------------------------------------

// ipBucketSweepInterval, bosalmis kovalarin ne siklikta temizlenecegi.
const ipBucketSweepInterval = time.Minute

type ipBucket struct {
	tokens float64
	last   time.Time
}

// ipRateLimiter, IP basina token-bucket hiz siniri.
//
// Token bucket secildi cunku haritanin GERCEK KULLANIM DESENI patlamalidir:
// kaydiriciyi suruklemek kisa surede birkac istek uretir, sonra kullanici
// durur. Sabit pencereli bir sayac bu patlamayi cezalandirir; token bucket
// biriken krediyle karsilar ve yalnizca SURDURULEN yuku keser.
//
// golang.org/x/time/rate yerine elle yazildi: tek bir yeni bagimlilik icin
// go.mod/go.sum degistirmeye degmiyor ve mantik 40 satir. Ayrica asagidaki
// hafiza siniri (maxIPs + sweep) x/time/rate'in kendi basina cozmedigi bir
// problem - kutuphaneyi alsak da o kismi yine yazacaktik.
type ipRateLimiter struct {
	rate   float64 // saniyede yeniden dolan token
	burst  float64 // kovanin tavani
	maxIPs int     // izlenen ayri IP sayisinin tavani

	// now, testlerin zamani kontrol edebilmesi icin ayrilabilir. nil ise
	// time.Now kullanilir.
	now func() time.Time

	mu        sync.Mutex
	buckets   map[string]*ipBucket
	lastSweep time.Time
}

func newIPRateLimiter(ratePerSec, burst, maxIPs int) *ipRateLimiter {
	return &ipRateLimiter{
		rate:    float64(ratePerSec),
		burst:   float64(burst),
		maxIPs:  maxIPs,
		buckets: make(map[string]*ipBucket),
	}
}

func (l *ipRateLimiter) clock() time.Time {
	if l.now != nil {
		return l.now()
	}
	return time.Now()
}

func (l *ipRateLimiter) allow(ip string) bool {
	now := l.clock()

	l.mu.Lock()
	defer l.mu.Unlock()

	// Temizlik EN BASTA yapiliyor, sonda degil: sweep dolmus kovalari siler ve
	// az asagida elimizde tuttugumuz kovayi silmesi, haritadan ayrilmis bir
	// yapiyi guncellememize yol acardi - degisiklik sessizce kaybolurdu.
	if now.Sub(l.lastSweep) >= ipBucketSweepInterval {
		l.sweepLocked(now)
	}

	b, ok := l.buckets[ip]
	if !ok {
		// Hafiza siniri: IP basina kayit tutan her yapi KENDISI bir DoS
		// vektorudur - saldirgan her istekte farkli bir kaynak gostererek
		// tabloyu sisirebilir. Once zorla temizle, yine yer yoksa reddet.
		// Kullanilabilirligi degil hafizayi korumak bilincli bir tercih:
		// tablo dolduysa zaten anormal bir durum yasaniyor demektir.
		if len(l.buckets) >= l.maxIPs {
			l.sweepLocked(now)
			if len(l.buckets) >= l.maxIPs {
				return false
			}
		}
		l.buckets[ip] = &ipBucket{tokens: l.burst - 1, last: now}
		return true
	}

	b.tokens += now.Sub(b.last).Seconds() * l.rate
	if b.tokens > l.burst {
		b.tokens = l.burst
	}
	b.last = now

	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}

// sweepLocked, tamamen dolmus kovalari siler.
//
// Silmek davranisi DEGISTIRMEZ: dolmus bir kova ile hic olmayan bir kova
// aynidir, cunku yeni bir IP de tam burst ile baslar. Yani bu saf bir hafiza
// geri kazanimi - sinirin sertligini etkilemez.
func (l *ipRateLimiter) sweepLocked(now time.Time) {
	l.lastSweep = now
	for ip, b := range l.buckets {
		if b.tokens+now.Sub(b.last).Seconds()*l.rate >= l.burst {
			delete(l.buckets, ip)
		}
	}
}

func (l *ipRateLimiter) size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.buckets)
}

// --- Es zamanli is siniri ---------------------------------------------------

// inFlightLimiter, ayni anda kac agir istegin islenebilecegini sinirlar.
//
// Hiz sinirinin TEK BASINA yetmedigi yer burasi: IP basina sinir dagitik bir
// yuke (ya da sadece kalabalik bir demo gunune) karsi bir sey yapmaz. Her
// /api/history istegi 9 bolge icin ayri ScyllaDB sorgusu aciyor; tavansiz
// birakmak yeterince es zamanli ziyaretcide ScyllaDB'yi doyurur ve o anda
// consumer'in yazma yolunu da yavaslatir - yani harita gecmisi yuzunden ANA
// BORU HATTI zarar gorur. Sinir tam da bunu keser.
type inFlightLimiter struct {
	slots chan struct{}
}

func newInFlightLimiter(n int) *inFlightLimiter {
	return &inFlightLimiter{slots: make(chan struct{}, n)}
}

func (f *inFlightLimiter) acquire() bool {
	select {
	case f.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (f *inFlightLimiter) release() { <-f.slots }

// --- HTTP ara katmani -------------------------------------------------------

// apiGuard, API uclarini hiz siniri ve es zamanlilik siniri ile sarar.
type apiGuard struct {
	limiter    *ipRateLimiter
	inFlight   *inFlightLimiter
	retryAfter int
}

// limit, yalnizca hiz siniri uygular (ucuz uclar icin).
func (g *apiGuard) limit(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !g.allowRate(w, r) {
			return
		}
		next(w, r)
	}
}

// limitHeavy, hiz siniri VE es zamanlilik siniri uygular (ScyllaDB'ye inen
// uclar icin).
func (g *apiGuard) limitHeavy(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !g.allowRate(w, r) {
			return
		}
		if !g.inFlight.acquire() {
			metricRejectedRequests.WithLabelValues("busy").Inc()
			g.reject(w, http.StatusServiceUnavailable, "sunucu su an mesgul")
			return
		}
		metricInFlight.Inc()
		defer func() {
			g.inFlight.release()
			metricInFlight.Dec()
		}()
		next(w, r)
	}
}

func (g *apiGuard) allowRate(w http.ResponseWriter, r *http.Request) bool {
	if g.limiter.allow(clientIP(r)) {
		return true
	}
	metricRejectedRequests.WithLabelValues("rate_limit").Inc()
	g.reject(w, http.StatusTooManyRequests, "cok fazla istek")
	return false
}

// reject, Retry-After ile birlikte reddeder.
//
// Retry-After bilincli: istemci tarafi (static/index.html) bu iki durum kodunu
// ayrica ele aliyor ve hemen yeniden denemek yerine geri cekiliyor. Basligi
// gondermeyen bir sunucu iyi niyetli istemcileri de sikisik bir donguye
// sokar - yani sinirin kendisi yuku artirir.
func (g *apiGuard) reject(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Retry-After", strconv.Itoa(g.retryAfter))
	http.Error(w, msg, status)
}
