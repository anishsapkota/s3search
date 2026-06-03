// generate produces synthetic EDIFACT messages in NDJSON format.
// Each line is a JSON object with fields: raw, message_type, sender_id,
// receiver_id, interchange_ref, bgm_ref, location, ts.
//
// Usage:
//
//	go run ./tools/generate -n 100000 -o edifact.ndjson
//	go run ./tools/generate -n 1000 -type UTILMD
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"time"
)

// ── Config ───────────────────────────────────────────────────────────────────

var (
	flagN    = flag.Int("n", 100_000, "number of messages to generate")
	flagOut  = flag.String("o", "", "output file (default: stdout)")
	flagType = flag.String("type", "", "restrict to one message type (e.g. UTILMD)")
	flagSeed = flag.Int64("seed", 0, "random seed (0 = time-based)")
)

var allTypes = []string{"UTILMD", "APERAK", "MSCONS", "UTILTS", "PRICAT", "INVOIC", "REMADV", "CONTRL"}

// ── Main ─────────────────────────────────────────────────────────────────────

func main() {
	flag.Parse()

	seed := *flagSeed
	if seed == 0 {
		seed = time.Now().UnixNano()
	}
	rng := rand.New(rand.NewSource(seed))

	types := allTypes
	if *flagType != "" {
		types = []string{*flagType}
	}

	out := os.Stdout
	if *flagOut != "" {
		f, err := os.Create(*flagOut)
		if err != nil {
			fmt.Fprintln(os.Stderr, "open output:", err)
			os.Exit(1)
		}
		defer f.Close()
		out = f
	}

	w := bufio.NewWriterSize(out, 1<<20)
	defer w.Flush()

	enc := json.NewEncoder(w)
	g := newGenerator(rng)

	for i := 0; i < *flagN; i++ {
		msgType := types[rng.Intn(len(types))]
		doc := g.generate(msgType)
		if err := enc.Encode(doc); err != nil {
			fmt.Fprintln(os.Stderr, "encode:", err)
			os.Exit(1)
		}
	}

	if *flagOut != "" {
		fmt.Fprintf(os.Stderr, "wrote %d messages to %s\n", *flagN, *flagOut)
	}
}

// ── Generator ────────────────────────────────────────────────────────────────

type Doc struct {
	Raw           string `json:"raw"`
	MessageType   string `json:"message_type"`
	SenderID      string `json:"sender_id"`
	ReceiverID    string `json:"receiver_id"`
	InterchangeRef string `json:"interchange_ref"`
	BGMRef        string `json:"bgm_ref"`
	Location      string `json:"location,omitempty"`
	TS            int64  `json:"ts"`
}

type generator struct {
	rng          *rand.Rand
	participants []string
	locations    []string
}

func newGenerator(rng *rand.Rand) *generator {
	g := &generator{rng: rng}
	g.participants = g.buildParticipants(60)
	g.locations = g.buildLocations(200)
	return g
}

func (g *generator) generate(msgType string) Doc {
	sender   := g.pick(g.participants)
	receiver := g.pickOther(g.participants, sender)
	ts       := g.randomTS()
	iref     := g.iref(ts)
	msgRef   := fmt.Sprintf("%d", g.rng.Int63n(900000)+100000)
	bgmRef   := fmt.Sprintf("%s%06d", iref[:10], g.rng.Int63n(1000000))
	dt       := time.UnixMilli(ts).UTC()

	var raw string
	switch msgType {
	case "UTILMD":
		raw = g.utilmd(sender, receiver, iref, msgRef, bgmRef, dt)
	case "APERAK":
		raw = g.aperak(sender, receiver, iref, msgRef, bgmRef, dt)
	case "MSCONS":
		raw = g.mscons(sender, receiver, iref, msgRef, bgmRef, dt)
	case "UTILTS":
		raw = g.utilts(sender, receiver, iref, msgRef, bgmRef, dt)
	case "PRICAT":
		raw = g.pricat(sender, receiver, iref, msgRef, bgmRef, dt)
	case "INVOIC":
		raw = g.invoic(sender, receiver, iref, msgRef, bgmRef, dt)
	case "REMADV":
		raw = g.remadv(sender, receiver, iref, msgRef, bgmRef, dt)
	case "CONTRL":
		raw = g.contrl(sender, receiver, iref, msgRef, bgmRef, dt)
	}

	loc := ""
	if msgType == "UTILMD" || msgType == "MSCONS" || msgType == "UTILTS" {
		loc = g.pick(g.locations)
	}

	return Doc{
		Raw:            raw,
		MessageType:    msgType,
		SenderID:       sender,
		ReceiverID:     receiver,
		InterchangeRef: iref,
		BGMRef:         bgmRef,
		Location:       loc,
		TS:             ts,
	}
}

// ── UTILMD ───────────────────────────────────────────────────────────────────

var utilmdBGM = []string{"Z07", "E01", "E02", "E03", "Z43", "Z44"}
var utilmdProcess = []string{"Z01", "Z02", "Z03", "Z04", "Z05", "Z06"}

func (g *generator) utilmd(sender, receiver, iref, msgRef, bgmRef string, dt time.Time) string {
	loc  := g.pick(g.locations)
	rff  := fmt.Sprintf("%06d", g.rng.Int63n(999999)+1)
	bgm  := g.pick(utilmdBGM)
	valid := dt.AddDate(0, 0, g.rng.Intn(365)+30)
	segs := []string{
		"UNA:+.? '",
		unb(sender, receiver, dt, iref),
		unh(msgRef, "UTILMD:D:11A:UN:S2.1"),
		fmt.Sprintf("BGM+%s+%s'", bgm, bgmRef),
		dtm(137, dt),
		nad("MS", sender),
		nad("MR", receiver),
		fmt.Sprintf("IDE+24+%s'", ideRef(g.rng)),
		dtm(159, valid),
		fmt.Sprintf("LOC+Z15+%s'", loc),
		fmt.Sprintf("RFF+Z13:%s'", rff),
		fmt.Sprintf("CAV+Z07+%s'", g.pick(utilmdProcess)),
		unt(11, msgRef),
		unz(1, iref),
	}
	return strings.Join(segs, "\n")
}

// ── APERAK ───────────────────────────────────────────────────────────────────

var aperakCodes = []string{"Z04", "Z07", "Z08", "Z09", "Z10", "Z11", "Z12", "Z13"}
var aperakText  = []string{
	"Zaehlpunkt nicht gefunden",
	"Marktpartner nicht bekannt",
	"Doppelte Nachrichtenreferenz",
	"Ungueltige Sparte",
	"Bilanzkreis nicht vorhanden",
	"Gerät nicht zugeordnet",
	"Netzanschluss unbekannt",
	"Ungültiges Datum",
}

func (g *generator) aperak(sender, receiver, iref, msgRef, bgmRef string, dt time.Time) string {
	origRef := fmt.Sprintf("%013d", g.rng.Int63n(9999999999999)+1000000000000)
	code    := g.pick(aperakCodes)
	text    := g.pick(aperakText)
	segs := []string{
		"UNA:+.? '",
		unb(sender, receiver, dt, iref),
		unh(msgRef, "APERAK:D:11A:UN:S2.1"),
		fmt.Sprintf("BGM+313+%s'", bgmRef),
		dtm(137, dt),
		nad("MS", sender),
		nad("MR", receiver),
		fmt.Sprintf("RFF+ACE:%s'", origRef),
		fmt.Sprintf("ERC+%s'", code),
		fmt.Sprintf("FTX+ABO+++%s'", text),
		unt(10, msgRef),
		unz(1, iref),
	}
	return strings.Join(segs, "\n")
}

// ── MSCONS ───────────────────────────────────────────────────────────────────

func (g *generator) mscons(sender, receiver, iref, msgRef, bgmRef string, dt time.Time) string {
	loc    := g.pick(g.locations)
	from   := dt.AddDate(0, -1, 0)
	to     := dt
	qty1   := fmt.Sprintf("%.3f", g.rng.Float64()*50000+100)
	qty2   := fmt.Sprintf("%.3f", g.rng.Float64()*10000)
	obis1  := g.obisCode()
	obis2  := g.obisCode()
	segs := []string{
		"UNA:+.? '",
		unb(sender, receiver, dt, iref),
		unh(msgRef, "MSCONS:D:04B:UN:2.1e"),
		fmt.Sprintf("BGM+7+%s'", bgmRef),
		dtm(137, dt),
		nad("MS", sender),
		nad("MR", receiver),
		fmt.Sprintf("NAD+DP+%s::293'", loc),
		fmt.Sprintf("LOC+Z15+%s'", loc),
		dtm(163, from),
		dtm(164, to),
		fmt.Sprintf("LIN+1'"),
		fmt.Sprintf("PIA+5+%s:SRW'", obis1),
		fmt.Sprintf("QTY+220:%s:KWH'", qty1),
		fmt.Sprintf("LIN+2'"),
		fmt.Sprintf("PIA+5+%s:SRW'", obis2),
		fmt.Sprintf("QTY+220:%s:KWH'", qty2),
		unt(16, msgRef),
		unz(1, iref),
	}
	return strings.Join(segs, "\n")
}

// ── UTILTS ───────────────────────────────────────────────────────────────────

func (g *generator) utilts(sender, receiver, iref, msgRef, bgmRef string, dt time.Time) string {
	loc   := g.pick(g.locations)
	from  := dt.Truncate(24 * time.Hour)
	segs  := []string{
		"UNA:+.? '",
		unb(sender, receiver, dt, iref),
		unh(msgRef, "UTILTS:D:11A:UN:S2.1"),
		fmt.Sprintf("BGM+Z09+%s'", bgmRef),
		dtm(137, dt),
		nad("MS", sender),
		nad("MR", receiver),
	}
	// Time series: 24 quarter-hour values
	for h := 0; h < 24; h++ {
		slot := from.Add(time.Duration(h) * time.Hour)
		val  := fmt.Sprintf("%.4f", g.rng.Float64()*500)
		segs = append(segs,
			fmt.Sprintf("SEQ+%d'", h+1),
			fmt.Sprintf("DTM+Z01:%s:303'", slot.Format("200601021504")),
			fmt.Sprintf("LOC+Z15+%s'", loc),
			fmt.Sprintf("QTY+Z01:%s:KWH'", val),
		)
	}
	segCount := len(segs) - 2 + 2 // UNH + UNT counted together
	segs = append(segs, unt(segCount, msgRef), unz(1, iref))
	return strings.Join(segs, "\n")
}

// ── PRICAT ───────────────────────────────────────────────────────────────────

var pricatProducts = []string{"Grundversorgung Strom HT", "Grundversorgung Strom NT", "Netzentgelt Niederspannung", "Messentgelt MSB", "Netznutzung Mittelspannung"}
var pricatUnits    = []string{"KWH", "MWH", "TAG", "MON"}

func (g *generator) pricat(sender, receiver, iref, msgRef, bgmRef string, dt time.Time) string {
	product := g.pick(pricatProducts)
	unit    := g.pick(pricatUnits)
	price   := fmt.Sprintf("%.4f", g.rng.Float64()*0.4+0.05)
	valid   := dt.AddDate(0, 0, g.rng.Intn(365)+30)
	segs := []string{
		"UNA:+.? '",
		unb(sender, receiver, dt, iref),
		unh(msgRef, "PRICAT:D:96A:UN:EAN008"),
		fmt.Sprintf("BGM+9+%s'", bgmRef),
		dtm(137, dt),
		nad("MS", sender),
		nad("BY", receiver),
		dtm(157, valid),
		fmt.Sprintf("LIN+1'"),
		fmt.Sprintf("IMD+F++:::%s'", product),
		fmt.Sprintf("PRI+AAB:%s:EUR::1:%s'", price, unit),
		fmt.Sprintf("TAX+7+VAT+++:::19+S'"),
		unt(12, msgRef),
		unz(1, iref),
	}
	return strings.Join(segs, "\n")
}

// ── INVOIC ───────────────────────────────────────────────────────────────────

func (g *generator) invoic(sender, receiver, iref, msgRef, bgmRef string, dt time.Time) string {
	due     := dt.AddDate(0, 0, 14)
	netAmt  := g.rng.Float64()*50000 + 100
	tax     := netAmt * 0.19
	gross   := netAmt + tax
	lineQty := fmt.Sprintf("%.3f", g.rng.Float64()*10000+100)
	lineNet := fmt.Sprintf("%.2f", netAmt)
	taxAmt  := fmt.Sprintf("%.2f", tax)
	grossAmt:= fmt.Sprintf("%.2f", gross)
	payRef  := fmt.Sprintf("RE%013d", g.rng.Int63n(9999999999999)+1000000000000)
	segs := []string{
		"UNA:+.? '",
		unb(sender, receiver, dt, iref),
		unh(msgRef, "INVOIC:D:96A:UN:EAN008"),
		fmt.Sprintf("BGM+380+%s+9'", bgmRef),
		dtm(137, dt),
		dtm(13, due),
		nad("SE", sender),
		nad("BY", receiver),
		fmt.Sprintf("PAT+1++5:3:D:14'"),
		fmt.Sprintf("CUX+2:EUR:4'"),
		fmt.Sprintf("LIN+1'"),
		fmt.Sprintf("QTY+47:%s:KWH'", lineQty),
		fmt.Sprintf("MOA+203:%s:EUR'", lineNet),
		fmt.Sprintf("TAX+7+VAT+++:::19+S'"),
		fmt.Sprintf("MOA+124:%s:EUR'", taxAmt),
		fmt.Sprintf("MOA+86:%s:EUR'", grossAmt),
		fmt.Sprintf("RFF+PQ:%s'", payRef),
		unt(17, msgRef),
		unz(1, iref),
	}
	return strings.Join(segs, "\n")
}

// ── REMADV ───────────────────────────────────────────────────────────────────

func (g *generator) remadv(sender, receiver, iref, msgRef, bgmRef string, dt time.Time) string {
	invRef  := fmt.Sprintf("RE%013d", g.rng.Int63n(9999999999999)+1000000000000)
	amount  := fmt.Sprintf("%.2f", g.rng.Float64()*100000+500)
	payRef  := fmt.Sprintf("ZV%013d", g.rng.Int63n(9999999999999)+1000000000000)
	segs := []string{
		"UNA:+.? '",
		unb(sender, receiver, dt, iref),
		unh(msgRef, "REMADV:D:96A:UN:EAN008"),
		fmt.Sprintf("BGM+481+%s+9'", bgmRef),
		dtm(137, dt),
		nad("PR", sender),
		nad("PB", receiver),
		fmt.Sprintf("RFF+PQ:%s'", payRef),
		dtm(140, dt),
		fmt.Sprintf("MOA+9:%s:EUR'", amount),
		fmt.Sprintf("DOC+380:%s'", invRef),
		fmt.Sprintf("MOA+12:%s:EUR'", amount),
		unt(12, msgRef),
		unz(1, iref),
	}
	return strings.Join(segs, "\n")
}

// ── CONTRL ───────────────────────────────────────────────────────────────────

var contrlSyntax = []string{"2", "4", "7", "12", "14"}

func (g *generator) contrl(sender, receiver, iref, msgRef, bgmRef string, dt time.Time) string {
	refIref := fmt.Sprintf("%013d", g.rng.Int63n(9999999999999)+1000000000000)
	action  := []string{"8", "27", "4"}[g.rng.Intn(3)] // 8=acknowledged, 27=rejected, 4=received
	segs := []string{
		"UNA:+.? '",
		unb(sender, receiver, dt, iref),
		unh(msgRef, "CONTRL:D:96A:UN"),
		fmt.Sprintf("UCI+%s+%s:500+%s:500+%s'", refIref, receiver, sender, action),
	}
	if action == "27" {
		code := g.pick(contrlSyntax)
		segs = append(segs,
			fmt.Sprintf("UCM+1+UTILMD:D:11A:UN:S2.1+5'"),
			fmt.Sprintf("UCS+1'"),
			fmt.Sprintf("UCD+%s+1'", code),
		)
	}
	segs = append(segs, unt(len(segs)-2, msgRef), unz(1, iref))
	return strings.Join(segs, "\n")
}

// ── Helpers ──────────────────────────────────────────────────────────────────

func unb(sender, receiver string, dt time.Time, iref string) string {
	return fmt.Sprintf("UNB+UNOC:3+%s:500+%s:500+%s:%s+%s'",
		sender, receiver,
		dt.Format("060102"), dt.Format("1504"),
		iref)
}

func unh(msgRef, msgType string) string {
	return fmt.Sprintf("UNH+%s+%s'", msgRef, msgType)
}

func unt(segCount int, msgRef string) string {
	return fmt.Sprintf("UNT+%d+%s'", segCount, msgRef)
}

func unz(msgCount int, iref string) string {
	return fmt.Sprintf("UNZ+%d+%s'", msgCount, iref)
}

func nad(qual, id string) string {
	return fmt.Sprintf("NAD+%s+%s::293'", qual, id)
}

func dtm(qual int, t time.Time) string {
	return fmt.Sprintf("DTM+%d:%s?+00:303'", qual, t.Format("200601021504"))
}

func (g *generator) iref(ts int64) string {
	return fmt.Sprintf("%013d", ts/1000%9999999999999+1000000000000)
}

func ideRef(rng *rand.Rand) string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 33)
	for i := range b {
		b[i] = chars[rng.Intn(len(chars))]
	}
	return "BEL" + string(b)
}

func (g *generator) obisCode() string {
	// Common OBIS codes for electricity metering
	codes := []string{
		"1-1:1.8.1", "1-1:1.8.2", "1-1:2.8.1", "1-1:2.8.2",
		"1-1:1.29.0", "1-1:2.29.0", "1-0:1.8.0", "1-0:2.8.0",
	}
	return g.pick(codes)
}

func (g *generator) randomTS() int64 {
	// Random date between 2023-01-01 and 2026-12-31
	start := time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC).UnixMilli()
	end   := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC).UnixMilli()
	return start + g.rng.Int63n(end-start)
}

// buildParticipants generates realistic BDEW/market participant IDs.
// German energy market: 13-digit numbers with common prefixes.
func (g *generator) buildParticipants(n int) []string {
	prefixes := []string{
		"9901", "9902", "9903", "9904", "9905",
		"4033", "4030", "4031", "4032",
		"9984", "9907", "9908", "9909",
	}
	ids := make([]string, n)
	seen := make(map[string]bool)
	for i := 0; i < n; {
		p := prefixes[g.rng.Intn(len(prefixes))]
		s := fmt.Sprintf("%09d", g.rng.Int63n(1000000000))
		id := p + s
		if len(id) == 13 && !seen[id] {
			ids[i] = id
			seen[id] = true
			i++
		}
	}
	return ids
}

// buildLocations generates DE metering point IDs (Zählpunktbezeichnungen).
func (g *generator) buildLocations(n int) []string {
	const chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	locs := make([]string, n)
	seen := make(map[string]bool)
	for i := 0; i < n; {
		b := make([]byte, 31)
		for j := range b {
			b[j] = chars[g.rng.Intn(len(chars))]
		}
		loc := "DE" + string(b)
		if !seen[loc] {
			locs[i] = loc
			seen[loc] = true
			i++
		}
	}
	return locs
}

func (g *generator) pick(s []string) string {
	return s[g.rng.Intn(len(s))]
}

func (g *generator) pickOther(s []string, exclude string) string {
	for i := 0; i < 20; i++ {
		v := g.pick(s)
		if v != exclude {
			return v
		}
	}
	return s[0]
}
