package Utils

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fatih/color"
)

var (

	// -- GENERAL
	Hit           int64
	Premium       int64
	Standard      int64
	Basic         int64
	CheckCounter  int64
	ComboLoaded   int64
	MaxRetry      int64
	MaxRetryCount int64
	Retries       int64
	ActiveWorkers int64
	Free          int64
	Factor        int64
	Expired       int64
	Fail          int64
	RetryAcc      int64
	Retry         int64
	ToCheck       int64
	Errors        int64
	Null          int64
	Removed              int64
	NoSlots              int64
	SendCode_ChangeEmail int64
	Hit_ChangeEmail      int64
	Hit_SubDisney        int64
	Hit_SubVix           int64
	Hit_Hotmail          int64
	Hit_Imap             int64
	HitSkiped            int64
	BanIPAcc             int64
	BadLines             int64
	Free_Disney          int64
	Free_Vix             int64
	Expired_Disney       int64
	Expired_Vix          int64
	Retry_SendCode       int64
	NullMsg_ChangeEmail  int64
	GetCode_ChangeEmail  int64
	Fail_ChangeEmail     int64

	// Guardado TXT
	writers             = make(map[string]chan string)
	writersWg           sync.WaitGroup
	writersClosed       atomic.Bool
	writersMu           sync.RWMutex
	currentDirPath      string
	currentWriterConfig WriterConfig
	mu                  sync.Mutex

	// Yahoo Palette Helpers
	Result     = fmt.Sprintf("Result [%s]", time.Now().Format("02-01-2006 15h04m05s"))
	Red        = color.New(color.FgHiRed).SprintFunc()
	Yellow     = color.New(color.FgHiYellow).SprintFunc()
	ColorLogo  = color.New(color.FgRed, color.Bold).SprintFunc()
	OrangeCo   = color.New(color.FgRed, color.FgYellow, color.Bold).SprintFunc()
	Purple     = color.New(color.FgHiMagenta, color.Bold).SprintFunc()
	Lilac      = color.New(color.FgMagenta, color.Bold).SprintFunc()
	Cyan       = color.New(color.FgHiCyan, color.Bold).SprintFunc()
	WhiteColor = color.New(color.FgHiWhite).SprintFunc()
	LimeGreen  = color.New(color.FgHiGreen).SprintFunc()

	// Para dominio generado con caracteres (Change Email)
	dominioGenerado       string
	DominiosMios          []string
	lineasSeleccionadas   = map[int]bool{}
	LineasSeleccionadasMu sync.Mutex
	FormaGmail            string
	NewDominioGen         *EmailVariantGenerator
	onceDominio           sync.Once
	NombreArchivoDominios = "MyDomains.txt"

	//strings
	ComboFilePath   string
	ComboFileName   string
	DetectedCountry string // País detectado en el combo (ej: "PE", "CL", "ES", "AR")

	lastHit     atomic.Pointer[string]
	lastBanIP   atomic.Pointer[string]
	lastFactor  atomic.Pointer[string]
	lastFree    atomic.Pointer[string]
	lastExpired atomic.Pointer[string]
	lastFail    atomic.Pointer[string]
	lastToCheck atomic.Pointer[string]
	lastRemoved atomic.Pointer[string]
	lastNoSlots atomic.Pointer[string]
	lastStepLog atomic.Pointer[string]

	recentCapturesMu sync.RWMutex
	recentCaptures   []LiveCapture
)

type LiveCapture struct {
	Timestamp string
	Type      string // "HIT", "FACTOR", "FAIL", "TOCHECK", "BANIP"
	Text      string
}

func AddLiveCapture(capType, text string) {
	recentCapturesMu.Lock()
	defer recentCapturesMu.Unlock()
	t := time.Now().Format("15:04:05")
	recentCaptures = append(recentCaptures, LiveCapture{Timestamp: t, Type: capType, Text: text})
	if len(recentCaptures) > 8 {
		recentCaptures = recentCaptures[len(recentCaptures)-8:]
	}
}

func GetRecentCaptures() []LiveCapture {
	recentCapturesMu.RLock()
	defer recentCapturesMu.RUnlock()
	res := make([]LiveCapture, len(recentCaptures))
	copy(res, recentCaptures)
	return res
}

func UpdateLastBanIP(value string) {
	lastBanIP.Store(&value)
	AddLiveCapture("BANIP", value)
}

func GetLastBanIP() string {
	p := lastBanIP.Load()
	if p == nil {
		return ""
	}
	return *p
}

// Configuración básica
type WriterConfig struct {
	BufferSize    int           // Tamaño del canal
	FlushInterval time.Duration // Cada cuánto forzar flush
}
type uiUpdate struct {
	eventType string
	value     string
}

func UpdateLastHit(value string) {
	lastHit.Store(&value)
	AddLiveCapture("HIT", value)
}

func GetLastHit() string {
	p := lastHit.Load()
	if p == nil {
		return ""
	}
	return *p
}

func UpdateLastFail(value string) {
	lastFail.Store(&value)
	AddLiveCapture("FAIL", value)
}

func GetLastFail() string {
	p := lastFail.Load()
	if p == nil {
		return ""
	}
	return *p
}

func UpdateLastFree(value string) {
	lastFree.Store(&value)
}

func GetLastFree() string {
	p := lastFree.Load()
	if p == nil {
		return ""
	}
	return *p
}

func UpdateLastExpired(value string) {
	lastExpired.Store(&value)
}

func GetLastExpired() string {
	p := lastExpired.Load()
	if p == nil {
		return ""
	}
	return *p
}

func UpdateLastFactor(value string) {
	lastFactor.Store(&value)
	AddLiveCapture("FACTOR", value)
}

func GetLastFactor() string {
	p := lastFactor.Load()
	if p == nil {
		return ""
	}
	return *p
}

func UpdateLastToCheck(value string) {
	lastToCheck.Store(&value)
	AddLiveCapture("TOCHECK", value)
}

func GetLastToCheck() string {
	p := lastToCheck.Load()
	if p == nil {
		return ""
	}
	return *p
}
