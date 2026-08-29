package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// --- clientIP ---------------------------------------------------------------

// Bu testin korudugu sey bir "detay" degil: en soldaki XFF girdisine guvenen
// bir hiz siniri, her istekte farkli bir sahte IP gonderilerek tamamen
// atlatilir. Yani yanlis tarafi secmek sinirin KENDISINI islevsiz kilar.
func TestClientIPSahteForwardedForaAldanmaz(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/api/history?at=1", nil)
	r.RemoteAddr = "172.19.0.9:41000" // Caddy
	// Istemci basligi uydurdu; Caddy kendi gordugu adresi SONA ekledi.
	r.Header.Set("X-Forwarded-For", "1.2.3.4, 203.0.113.7")

	if got := clientIP(r); got != "203.0.113.7" {
		t.Fatalf("proxy'nin ekledigi adres beklenirdi, alinan: %q", got)
	}
}

func TestClientIPTekGirdiVeBosluk(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "172.19.0.9:41000"
	r.Header.Set("X-Forwarded-For", "  203.0.113.7  ")

	if got := clientIP(r); got != "203.0.113.7" {
		t.Fatalf("beklenen 203.0.113.7, alinan %q", got)
	}
}

// XFF yoksa (local gelistirme: port dogrudan disa acik) RemoteAddr'a dusulur.
func TestClientIPForwardedForYoksaRemoteAddr(t *testing.T) {
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.RemoteAddr = "198.51.100.4:52344"

	if got := clientIP(r); got != "198.51.100.4" {
		t.Fatalf("beklenen 198.51.100.4, alinan %q", got)
	}
}

// --- ipRateLimiter ----------------------------------------------------------

// sahteSaat, testlerin gercek zamani beklemeden token dolumunu
// dogrulayabilmesi icin.
type sahteSaat struct{ t time.Time }

func (s *sahteSaat) simdi() time.Time       { return s.t }
func (s *sahteSaat) ilerle(d time.Duration) { s.t = s.t.Add(d) }

func yeniLimiter(rate, burst, maxIPs int) (*ipRateLimiter, *sahteSaat) {
	saat := &sahteSaat{t: time.Unix(1_700_000_000, 0)}
	l := newIPRateLimiter(rate, burst, maxIPs)
	l.now = saat.simdi
	return l, saat
}

func TestRateLimiterPatlamayiGecirirSonrasiniKeser(t *testing.T) {
	l, _ := yeniLimiter(10, 3, 100)

	for i := 0; i < 3; i++ {
		if !l.allow("1.1.1.1") {
			t.Fatalf("patlama kotasi icindeki %d. istek gecmeliydi", i+1)
		}
	}
	if l.allow("1.1.1.1") {
		t.Fatal("patlama kotasi bitince istek reddedilmeliydi")
	}
}

func TestRateLimiterZamanlaYenidenDolar(t *testing.T) {
	l, saat := yeniLimiter(10, 3, 100)

	for i := 0; i < 3; i++ {
		l.allow("1.1.1.1")
	}
	if l.allow("1.1.1.1") {
		t.Fatal("kota bitmis olmaliydi")
	}

	// 10 token/sn -> 100ms tam olarak 1 token eder.
	saat.ilerle(100 * time.Millisecond)
	if !l.allow("1.1.1.1") {
		t.Fatal("100ms sonra 1 token dolmus olmaliydi")
	}
	if l.allow("1.1.1.1") {
		t.Fatal("yalnizca 1 token dolmustu, ikinci istek gecmemeliydi")
	}
}

func TestRateLimiterIPleriAyirir(t *testing.T) {
	l, _ := yeniLimiter(10, 2, 100)

	l.allow("1.1.1.1")
	l.allow("1.1.1.1")
	if l.allow("1.1.1.1") {
		t.Fatal("ilk IP'nin kotasi bitmis olmaliydi")
	}
	if !l.allow("2.2.2.2") {
		t.Fatal("ikinci IP ilkinin kotasindan etkilenmemeliydi")
	}
}

// Hafiza siniri: dolmus kovalar temizlenmezse tablo suresiz buyur ve hiz
// sinirinin kendisi bir DoS vektorune donusur.
func TestRateLimiterDolmusKovalariTemizler(t *testing.T) {
	l, saat := yeniLimiter(10, 3, 100)

	l.allow("1.1.1.1")
	if l.size() != 1 {
		t.Fatalf("1 kova beklenirdi, %d var", l.size())
	}

	// Sweep araligini gec: 1.1.1.1'in kovasi bu sure icinde tamamen dolar.
	saat.ilerle(ipBucketSweepInterval + time.Second)
	l.allow("2.2.2.2")

	if l.size() != 1 {
		t.Fatalf("dolmus kova silinmeliydi, geriye yalnizca yeni IP kalmaliydi; %d kova var", l.size())
	}
}

// Temizlik yer acamiyorsa yeni IP reddedilir - kullanilabilirlik degil hafiza
// korunur. Bilincli bir takas; tablo dolduysa zaten anormal bir durum vardir.
func TestRateLimiterTabloDoluykenYeniIPReddedilir(t *testing.T) {
	// rate=0: kovalar hic dolmaz, dolayisiyla sweep hicbir sey silemez.
	l, _ := yeniLimiter(0, 5, 2)

	if !l.allow("1.1.1.1") || !l.allow("2.2.2.2") {
		t.Fatal("tablo tavanina kadar olan IP'ler kabul edilmeliydi")
	}
	if l.allow("3.3.3.3") {
		t.Fatal("tablo doluyken yeni IP reddedilmeliydi")
	}
	if l.size() != 2 {
		t.Fatalf("tablo tavani asilmis: %d", l.size())
	}
}

// --- inFlightLimiter --------------------------------------------------------

func TestInFlightLimiterTavaniKorur(t *testing.T) {
	f := newInFlightLimiter(2)

	if !f.acquire() || !f.acquire() {
		t.Fatal("tavan icindeki iki slot alinabilmeliydi")
	}
	if f.acquire() {
		t.Fatal("tavan doluyken slot verilmemeliydi")
	}

	f.release()
	if !f.acquire() {
		t.Fatal("birakilan slot yeniden alinabilmeliydi")
	}
}

// --- apiGuard ---------------------------------------------------------------

func yeniGuard(rate, burst, inFlight int) *apiGuard {
	return &apiGuard{
		limiter:    newIPRateLimiter(rate, burst, 100),
		inFlight:   newInFlightLimiter(inFlight),
		retryAfter: 5,
	}
}

func istek(ip string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/api/history?at=1", nil)
	r.RemoteAddr = ip + ":40000"
	return r
}

func TestGuardHizSiniriAsilinca429VeRetryAfter(t *testing.T) {
	g := yeniGuard(10, 1, 4)
	h := g.limit(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	ilk := httptest.NewRecorder()
	h(ilk, istek("203.0.113.10"))
	if ilk.Code != http.StatusOK {
		t.Fatalf("ilk istek gecmeliydi, alinan %d", ilk.Code)
	}

	ikinci := httptest.NewRecorder()
	h(ikinci, istek("203.0.113.10"))
	if ikinci.Code != http.StatusTooManyRequests {
		t.Fatalf("beklenen 429, alinan %d", ikinci.Code)
	}
	// Retry-After olmadan iyi niyetli istemci de hemen yeniden dener; yani
	// sinirin kendisi yuku artirir.
	if ikinci.Header().Get("Retry-After") == "" {
		t.Fatal("429 yanitinda Retry-After basligi bulunmaliydi")
	}
}

// Es zamanlilik siniri hiz sinirindan BAGIMSIZ calismali: farkli IP'lerden
// gelen istekler hiz sinirine takilmaz ama ScyllaDB'yi yine de doyurabilir.
func TestGuardEsZamanlilikTavaniFarkliIPlerdeDeUygulanir(t *testing.T) {
	g := yeniGuard(100, 100, 1)

	girdi := make(chan struct{})
	birak := make(chan struct{})
	h := g.limitHeavy(func(w http.ResponseWriter, r *http.Request) {
		close(girdi)
		<-birak
	})

	go h(httptest.NewRecorder(), istek("203.0.113.11"))
	<-girdi // ilk istek slotu tutuyor

	ikinci := httptest.NewRecorder()
	h(ikinci, istek("203.0.113.12")) // BASKA bir IP: hiz sinirine takilmaz
	if ikinci.Code != http.StatusServiceUnavailable {
		t.Fatalf("beklenen 503, alinan %d", ikinci.Code)
	}

	close(birak)
}

func TestGuardSlotIsBittiktenSonraGeriVerilir(t *testing.T) {
	g := yeniGuard(100, 100, 1)
	h := g.limitHeavy(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	for i := 0; i < 3; i++ {
		rec := httptest.NewRecorder()
		h(rec, istek("203.0.113.13"))
		if rec.Code != http.StatusOK {
			t.Fatalf("%d. istek 200 donmeliydi (slot sizintisi?), alinan %d", i+1, rec.Code)
		}
	}
}

// --- originAllowed ----------------------------------------------------------

func wsIstek(host, origin string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/ws", nil)
	r.Host = host
	if origin != "" {
		r.Header.Set("Origin", origin)
	}
	return r
}

func TestOriginAyniKaynakKabulEdilir(t *testing.T) {
	// Alan adi olmayan (IP ile servis edilen) kurulum da yapilandirma
	// gerektirmeden calismali.
	if !originAllowed(wsIstek("203.0.113.5", "http://203.0.113.5")) {
		t.Fatal("ayni kaynak kabul edilmeliydi")
	}
	// TLS'li kurulumda sema https; yalnizca host karsilastirdigimiz icin gecer.
	if !originAllowed(wsIstek("harita.example.org", "https://harita.example.org")) {
		t.Fatal("https ayni kaynak kabul edilmeliydi")
	}
}

func TestOriginBaskaSiteReddedilir(t *testing.T) {
	if originAllowed(wsIstek("harita.example.org", "https://baskasite.example.com")) {
		t.Fatal("capraz-site WebSocket acilisi reddedilmeliydi")
	}
}

func TestOriginListedekiHostKabulEdilir(t *testing.T) {
	eski := extraAllowedOrigins
	extraAllowedOrigins = []string{"pano.example.org"}
	defer func() { extraAllowedOrigins = eski }()

	if !originAllowed(wsIstek("harita.example.org", "https://pano.example.org")) {
		t.Fatal("ALLOWED_ORIGINS'teki host kabul edilmeliydi")
	}
}

func TestOriginBasligiYoksaKabulEdilir(t *testing.T) {
	// curl, testler, saglik kontrolu Origin gondermez.
	if !originAllowed(wsIstek("harita.example.org", "")) {
		t.Fatal("Origin'siz istek kabul edilmeliydi")
	}
}

// --- hub istemci tavani -----------------------------------------------------

func TestHubIstemciTavaniniAsmaz(t *testing.T) {
	h := newHub(100)
	h.maxClients = 2

	for i := 0; i < 2; i++ {
		if !h.addClient(&client{send: make(chan []byte, 1)}) {
			t.Fatalf("tavan icindeki %d. istemci kabul edilmeliydi", i+1)
		}
	}
	if h.addClient(&client{send: make(chan []byte, 1)}) {
		t.Fatal("tavan doluyken istemci reddedilmeliydi")
	}
	if h.clientCount() != 2 {
		t.Fatalf("tavan asilmis: %d istemci", h.clientCount())
	}
}

// maxClients ayarlanmamis bir hub (mevcut testlerin kullandigi hal) eskisi gibi
// sinirsiz davranmali - yeni alan var olan davranisi degistirmemeli.
func TestHubTavanSifirsaSinirsiz(t *testing.T) {
	h := newHub(100)
	for i := 0; i < 50; i++ {
		if !h.addClient(&client{send: make(chan []byte, 1)}) {
			t.Fatalf("tavan 0 iken %d. istemci reddedildi", i+1)
		}
	}
}
