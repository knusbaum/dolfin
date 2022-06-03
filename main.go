package main

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/polygon-io/xbrl-parser"
	"golang.org/x/net/html/charset"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const UserAgent = "Experimental Crawler. Contact Kyle Nusbaum at kjn@9project.net"

var companyLog *log.Logger
var accessionLog *log.Logger
var factLog *log.Logger
var fetchLog *log.Logger

type Company struct {
	ID     int
	Ticker string `gorm:"index:company_ticker_cik,unique"`
	CIK    int    `gorm:"index:company_ticker_cik,unique"`
	Name   string
}

type Filing struct {
	ID        int
	CIK       int    `gorm:"index:filing_cik_accession_url,unique"`
	Accession string `gorm:"index:filing_cik_accession_url,unique"`
	URL       string `gorm:"index:filing_cik_accession_url,unique"`
}

type FilingEntry struct {
	ID        int
	FilingID  int
	CIK       int    `gorm:"index:filingentry_cik_accession_url,unique"`
	Accession string `gorm:"index:filingentry_cik_accession_url,unique"`
	URL       string `gorm:"index:filingentry_cik_accession_url,unique"`
}

//fmt.Printf("Fact: %s:%s (type: %s) from entity: (%v)\n", fact.XMLName.Space, fact.XMLName.Local, factType, factContext.Entity)
type Fact struct {
	CIK           int
	FilingEntryID int
	Space         string
	Local         string
	Type          string
	Value         float64
	Unit          string
	Immediate     time.Time
	Start         time.Time
	End           time.Time
}

// LoadCompanies downloads the list of companies' tickers and CIK numbers from the SEC and updates the database.
func LoadCompanies(db *gorm.DB) error {
	resp, err := http.Get("https://www.sec.gov/files/company_tickers.json")
	if err != nil {
		return err
	}
	v := make(map[int]struct {
		CikStr int `json:"cik_str"`
		Ticker string
		Title  string
	})

	d := json.NewDecoder(resp.Body)
	defer resp.Body.Close()
	err = d.Decode(&v)
	if err != nil {
		return err
	}

	companies := make([]Company, 0, len(v))
	for _, v := range v {
		companies = append(companies, Company{Name: v.Title, Ticker: v.Ticker, CIK: v.CikStr})
	}
	tx := db.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(companies, 100)
	if tx.Error != nil {
		//log.Printf("Failed to insert companies: %v", tx.Error)
		return tx.Error
	}

	return nil
}

func main() {
	go runPullLimiter()
	db, err := gorm.Open(sqlite.Open("test.db"), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}
	if err := db.AutoMigrate(&Company{}); err != nil {
		log.Fatalf("Error migrating: %v\n", err)
	}
	if err := db.AutoMigrate(&Filing{}); err != nil {
		log.Fatalf("Error migrating: %v\n", err)
	}
	if err := db.AutoMigrate(&FilingEntry{}); err != nil {
		log.Fatalf("Error migrating: %v\n", err)
	}
	if err := db.AutoMigrate(&Fact{}); err != nil {
		log.Fatalf("Error migrating: %v\n", err)
	}
	if err := LoadCompanies(db); err != nil {
		log.Fatalf("Failed to load companies: %v\n", err)
	}
	f, err := os.Create("company.log")
	if err != nil {
		log.Fatalf("Failed to create company log")
	}
	defer f.Close()
	companyLog = log.New(f, "COMPANY ", log.Ldate|log.Ltime)
	f, err = os.Create("accession.log")
	if err != nil {
		log.Fatalf("Failed to create accession log")
	}
	defer f.Close()
	accessionLog = log.New(f, "ACCESSION ", log.Ldate|log.Ltime)
	f, err = os.Create("fact.log")
	if err != nil {
		log.Fatalf("Failed to create fact log")
	}
	defer f.Close()
	factLog = log.New(f, "FACT ", log.Ldate|log.Ltime)

	f, err = os.Create("fetch.log")
	if err != nil {
		log.Fatalf("Failed to create fetch log")
	}
	defer f.Close()
	fetchLog = log.New(f, "FETCH ", log.Ldate|log.Ltime)

	var companies []Company
	//tx := db.Where("cik = ?", 51143).Find(&companies)
	tx := db.Find(&companies)
	if tx.Error != nil {
		log.Fatalf("Failed to query companies: %v", err)
	}

	workChan := make(chan func(), 1)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range workChan {
				f()
			}
			fmt.Printf("Shutting down worker\n")
		}()
	}

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc, os.Interrupt)
	// 	go func() {
	// 		for sig := range c {
	// 			// sig is a ^C, handle it
	// 		}
	// 	}()

	ctx, cancel := context.WithCancel(context.Background())
loop:
	for i, c := range companies {
		cc := c
		ii := i
		select {
		case workChan <- func(i int, c Company) func() {
			return func() {
				companyLog.Printf("Processing company %v (CIK: %d) (%d/%d)", cc.Name, cc.CIK, ii, len(companies))
				processCompany(ctx, db, cc.CIK)
				companyLog.Printf("Finished company %v (CIK: %d) (%d/%d)", cc.Name, cc.CIK, ii, len(companies))
			}
		}(i, c):
		case <-sigc:
			companyLog.Printf("Shutting down...")
			fmt.Printf("Shutting down...")
			cancel()
			go func() {
				<-sigc
				os.Exit(1)
			}()
			break loop
		}

	}
	close(workChan)
	companyLog.Printf("Waiting for Companies to finish.\n")
	wg.Wait()
}

func processCompany(ctx context.Context, db *gorm.DB, cik int) error {
	ss, err := listaccession(cik)
	if err != nil {
		return err
	}
	for _, s := range ss {
		if ctx.Err() != nil {
			return nil
		}
		ss2, filing, err := listxmlcontents(db, cik, s)
		if err != nil {
			companyLog.Printf("Failed to list XML files for CIK: %v, accession: %s: %v\n", cik, s, err)
			continue
		}
		for _, s2 := range ss2 {
			accessionLog.Printf("Processing File: %v\n", s2)
			err = parse(db, filing.ID, cik, s, s2)
			if err != nil {
				accessionLog.Printf("Failed to parse %v: %v\n", s2, err)
			}
		}
	}
	return nil
}

type Directory struct {
	XMLName xml.Name `xml:"html"`
	Body    DirBody  `xml:"body"`
}

type DirBody struct {
	XMLName   xml.Name      `xml:"directory"`
	Directory DirectoryElem `xml:"directory"`
}

type DirectoryElem struct {
	XMLName xml.Name `xml:"directory"`
	Name    string   `xml:"name"`
	Items   []Item   `xml:"item"`
}

type Item struct {
	XMLName      xml.Name `xml:"item"`
	Name         string   `xml:"name"`
	Size         string   `xml:"size"`
	Href         string   `xml:"href"`
	LastModified string   `xml:"last-modified"`
}

var pullLimiter chan struct{}

func runPullLimiter() {
	pullLimiter = make(chan struct{}, 8)
	ticker := time.Tick(125 * time.Millisecond)
	for _ = range ticker {
		pullLimiter <- struct{}{}
	}
}

func waitForLimit(who string) {
	select {
	case <-pullLimiter:
		fetchLog.Printf("[%s] OK\n", who)
		return
	default:
		fetchLog.Printf("[%s] Waiting for pull limit.\n", who)
		select {
		case <-pullLimiter:
			fetchLog.Printf("[%s] OK\n", who)
			return
		}
	}
}

func listaccession(cik int) ([]string, error) {
	waitForLimit(fmt.Sprintf("Listing All Filings for CIK %d", cik))
	r, err := http.NewRequest("GET", fmt.Sprintf("https://www.sec.gov/Archives/edgar/data/%d/index.xml", cik), nil)
	if err != nil {
		return nil, err
	}
	r.Header.Add("User-Agent", UserAgent)

	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	d := xml.NewDecoder(resp.Body)
	var m DirectoryElem
	err = d.Decode(&m)
	if err != nil {
		return nil, err
	}
	var ss []string
	mmm := make(map[string]struct{})
	for _, i := range m.Items {
		if _, ok := mmm[i.Name]; ok {
			accessionLog.Printf("Found duplicate directory (%v) in index %s\n", i.Name, fmt.Sprintf("https://www.sec.gov/Archives/edgar/data/%d/index.xml", cik))
		} else {
			mmm[i.Name] = struct{}{}
			ss = append(ss, i.Name)
		}
	}
	return ss, nil
}

func listxmlcontents(db *gorm.DB, cik int, accession string) ([]string, *Filing, error) {
	url := fmt.Sprintf("https://www.sec.gov/Archives/edgar/data/%d/%s/index.xml", cik, accession)
	filing := Filing{
		CIK:       cik,
		Accession: accession,
		URL:       url,
	}

	result := db.Create(&filing)
	if result.Error != nil {
		return nil, nil, result.Error
	}

	waitForLimit(fmt.Sprintf("Listing All XML Files for CIK %d, Accession %s", cik, accession))
	r, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, nil, err
	}
	r.Header.Add("User-Agent", UserAgent)

	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return nil, nil, err
	}
	defer resp.Body.Close()

	d := xml.NewDecoder(resp.Body)
	var m DirectoryElem
	err = d.Decode(&m)
	if err != nil {
		return nil, nil, err
	}
	var ss []string
	for _, i := range m.Items {
		if strings.HasSuffix(i.Href, ".xml") {
			ss = append(ss, fmt.Sprintf("https://www.sec.gov%s", i.Href))
		}
	}
	return ss, &filing, nil
}

func parse(db *gorm.DB, filingID int, cik int, accession string, url string) error {
	filing := FilingEntry{
		FilingID:  filingID,
		CIK:       cik,
		Accession: accession,
		URL:       url,
	}
	result := db.Create(&filing)
	if result.Error != nil {
		return result.Error
	}

	waitForLimit(fmt.Sprintf("Processing XML File from CIK %d, Accession %s, (%s)", cik, accession, url))
	var processed xbrl.XBRL
	r, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return err
	}
	r.Header.Add("User-Agent", UserAgent)

	resp, err := http.DefaultClient.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	d := xml.NewDecoder(resp.Body)
	d.CharsetReader = charset.NewReaderLabel
	err = d.Decode(&processed)
	if err != nil {
		return err
	}

	insertFacts := make([]Fact, 0, len(processed.Facts))
	for _, fact := range processed.Facts {
		//fact := processed.Facts[0]
		if !fact.IsValid() {
			//log.Printf("fact invalid!: %#v", fact)
			continue
		}

		factType := fact.Type()
		numericValue, err := fact.NumericValue()
		if err != nil {
			//log.Printf("Error: %v", err)
			continue
		}

		factContext := processed.ContextsByID[fact.ContextRef]
		factUnit := xbrl.Unit{ID: "Unknown"}
		if fact.UnitRef != nil {
			factUnit = processed.UnitsByID[*fact.UnitRef]
		}

		//	fmt.Printf("Fact: %#v (%v, %v, %v, %v)\n", fact, factType, factContext, factUnit, numericValue)

		f := Fact{
			CIK:           cik,
			FilingEntryID: filing.ID,
			Space:         fact.XMLName.Space,
			Local:         fact.XMLName.Local,
			Type:          string(factType),
			Value:         numericValue,
			Unit:          factUnit.String(),
		}
		switch factContext.Period.Type() {
		case xbrl.PeriodTypeDuration:
			t, err := time.Parse("2006-01-02", *factContext.Period.StartDate)
			if err != nil {
				factLog.Printf("Failed to parse date: %v", err)
				continue
			}
			f.Start = t
			t, err = time.Parse("2006-01-02", *factContext.Period.EndDate)
			if err != nil {
				factLog.Printf("Failed to parse date: %v", err)
				continue
			}
			f.End = t
		case xbrl.PeriodTypeInstant:
			t, err := time.Parse("2006-01-02", *factContext.Period.Instant)
			if err != nil {
				factLog.Printf("Failed to parse date: %v", err)
				continue
			}
			f.Immediate = t
		case xbrl.PeriodTypeForever:
			//fmt.Printf("      %.0f %s FOREVER\n", numericValue, factUnit.String())
		default:
			factLog.Printf("      UNKNOWN CONTEXT\n")
		}
		insertFacts = append(insertFacts, f)
		// 		fmt.Printf("Fact: %s:%s (type: %s) from entity: (%v)\n", fact.XMLName.Space, fact.XMLName.Local, factType, factContext.Entity)
		// 		switch factContext.Period.Type() {
		// 		case xbrl.PeriodTypeDuration:
		// 			fmt.Printf("      %.0f %s from %s to %s\n", numericValue, factUnit.String(), *factContext.Period.StartDate, *factContext.Period.EndDate)
		// 		case xbrl.PeriodTypeInstant:
		//
		// 			fmt.Printf("      %.0f %s on %s\n", numericValue, factUnit.String(), *factContext.Period.Instant)
		// 		case xbrl.PeriodTypeForever:
		// 			fmt.Printf("      %.0f %s FOREVER\n", numericValue, factUnit.String())
		// 		default:
		// 			fmt.Printf("      UNKNOWN CONTEXT\n")
		// 		}
	}
	factLog.Printf("Adding %d facts.", len(insertFacts))
	if len(insertFacts) > 0 {
		result = db.CreateInBatches(insertFacts, 100)
		if result.Error != nil {
			return result.Error
		}
	}
	return nil
}
