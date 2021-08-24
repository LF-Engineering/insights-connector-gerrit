package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/LF-Engineering/insights-datasource-gerrit/gen/models"
	shared "github.com/LF-Engineering/insights-datasource-shared"
	jsoniter "github.com/json-iterator/go"
	// jsoniter "github.com/json-iterator/go"
)

const (
	// GerritBackendVersion - backend version
	GerritBackendVersion = "0.1.1"
	// GerritDefaultSSHKeyPath - default path to look for gerrit ssh private key
	GerritDefaultSSHKeyPath = "$HOME/.ssh/id_rsa"
	// GerritDefaultSSHPort - default gerrit ssh port
	GerritDefaultSSHPort = 29418
	// GerritDefaultMaxReviews = default max reviews when processing gerrit
	GerritDefaultMaxReviews = 1000
	// GerritCodeReviewApprovalType - code review approval type
	GerritCodeReviewApprovalType = "Code-Review"
)

var (
	// GerritCategories - categories defined for gerrit
	GerritCategories = map[string]struct{}{"review": {}}
	// GerritVersionRegexp - gerrit verion pattern
	GerritVersionRegexp = regexp.MustCompile(`gerrit version (\d+)\.(\d+).*`)
	// GerritDefaultSearchField - default search field
	GerritDefaultSearchField = "item_id"
	// GerritReviewRoles - roles to fetch affiliation data for review
	GerritReviewRoles = []string{"owner"}
	// GerritCommentRoles - roles to fetch affiliation data for comment
	GerritCommentRoles = []string{"reviewer"}
	// GerritPatchsetRoles - roles to fetch affiliation data for patchset
	GerritPatchsetRoles = []string{"author", "uploader"}
	// GerritApprovalRoles - roles to fetch affiliation data for approval
	GerritApprovalRoles = []string{"by"}
	// max upstream date
	gMaxUpstreamDt    time.Time
	gMaxUpstreamDtMtx = &sync.Mutex{}
	// GerritDataSource - constant
	GerritDataSource = &models.DataSource{Name: "Gerrit", Slug: "gerrit"}
	gGerritMetaData  = &models.MetaData{BackendName: "gerrit", BackendVersion: GerritBackendVersion}
)

// DSGerrit - DS implementation for stub - does nothing at all, just presents a skeleton code
type DSGerrit struct {
	URL                 string // gerrit ptah, for example gerrit.onap.org
	User                string // gerrit user name
	SSHKey              string // must contain full SSH private key - has higher priority than key path
	SSHKeyPath          string // path to SSH private key, default GerritDefaultSSHKeyPath '~/.ssh/id_rsa'
	SSHPort             int    // gerrit port defaults to GerritDefaultSSHPort (29418)
	MaxReviews          int    // max reviews pack size defaults to GerritDefaultMaxReviews (1000)
	DisableHostKeyCheck bool   // disable host key check
	// Flags
	FlagURL                 *string
	FlagUser                *string
	FlagSSHKey              *string
	FlagSSHKeyPath          *string
	FlagSSHPort             *int
	FlagMaxReviews          *int
	FlagDisableHostKeyCheck *bool
	// Non-config variables
	SSHOpts        string   // SSH Options
	SSHKeyTempPath string   // if used SSHKey - temp file with this name was used to store key contents
	GerritCmd      []string // gerrit remote command used to fetch data
	VersionMajor   int      // gerrit major version
	VersionMinor   int      // gerrit minor version
}

// AddFlags - add Gerrit specific flags
func (j *DSGerrit) AddFlags() {
	j.FlagURL = flag.String("gerrit-url", "", "gerrit ptah, for example gerrit.onap.org")
	j.FlagUser = flag.String("gerrit-user", "", "gerrit user name")
	j.FlagSSHKey = flag.String("gerrit-ssh-key", "", "must contain full SSH private key - has higher priority than key path")
	j.FlagSSHKeyPath = flag.String("gerrit-ssh-key-path", GerritDefaultSSHKeyPath, "path to SSH private key, default '"+GerritDefaultSSHKeyPath+"'")
	j.FlagSSHPort = flag.Int("gerrit-ssh-port", GerritDefaultSSHPort, fmt.Sprintf("gerrit port defaults to GerritDefaultSSHPort (%d)", GerritDefaultSSHPort))
	j.FlagMaxReviews = flag.Int("gerrit-max-reviews", GerritDefaultMaxReviews, fmt.Sprintf("max reviews pack size defaults to GerritDefaultMaxReviews (%d)", GerritDefaultMaxReviews))
	j.FlagDisableHostKeyCheck = flag.Bool("gerrit-disable-host-key-check", false, "disable host key check")
}

// ParseArgs - parse gerrit specific environment variables
func (j *DSGerrit) ParseArgs(ctx *shared.Ctx) (err error) {
	// Gerrit URL
	if shared.FlagPassed(ctx, "url") && *j.FlagURL != "" {
		j.URL = *j.FlagURL
	}
	if ctx.EnvSet("URL") {
		j.URL = ctx.Env("URL")
	}

	// User
	if shared.FlagPassed(ctx, "user") && *j.FlagUser != "" {
		j.User = *j.FlagUser
	}
	if ctx.EnvSet("USER") {
		j.User = ctx.Env("USER")
	}
	if j.User != "" {
		shared.AddRedacted(j.User, false)
	}

	// Key Path
	j.SSHKeyPath = GerritDefaultSSHKeyPath
	if shared.FlagPassed(ctx, "ssh-key-path") && *j.FlagSSHKeyPath != "" {
		j.SSHKeyPath = *j.FlagSSHKeyPath
	}
	if ctx.EnvSet("SSH_KEY_PATH") {
		j.SSHKeyPath = ctx.Env("SSH_KEY_PATH")
	}

	// Key
	if shared.FlagPassed(ctx, "ssh-key") && *j.FlagSSHKey != "" {
		j.SSHKey = *j.FlagSSHKey
	}
	if ctx.EnvSet("SSH_KEY") {
		j.SSHKey = ctx.Env("SSH_KEY")
	}
	if j.SSHKey != "" {
		shared.AddRedacted(j.SSHKey, false)
	}

	// Disable host key check
	if shared.FlagPassed(ctx, "disable-host-key-check") {
		j.DisableHostKeyCheck = *j.FlagDisableHostKeyCheck
	}
	disableHostKeyCheck, present := ctx.BoolEnvSet("DISABLE_HOST_KEY_CHECK")
	if present {
		j.DisableHostKeyCheck = disableHostKeyCheck
	}

	// Max reviews
	passed := shared.FlagPassed(ctx, "max-reviews")
	if passed {
		j.MaxReviews = *j.FlagMaxReviews
	}
	if ctx.EnvSet("MAX_REVIEWS") {
		maxReviews, err := strconv.Atoi(ctx.Env("MAX_REVIEWS"))
		shared.FatalOnError(err)
		if maxReviews > 0 {
			j.MaxReviews = maxReviews
		}
	} else if !passed {
		j.MaxReviews = GerritDefaultMaxReviews
	}

	// SSH Port
	passed = shared.FlagPassed(ctx, "ssh-port")
	if passed {
		j.SSHPort = *j.FlagSSHPort
	}
	if ctx.EnvSet("SSH_PORT") {
		sshPort, err := strconv.Atoi(ctx.Env("SSH_PORT"))
		shared.FatalOnError(err)
		if sshPort > 0 {
			j.SSHPort = sshPort
		}
	} else if !passed {
		j.SSHPort = GerritDefaultSSHPort
	}
	return
}

// Validate - is current DS configuration OK?
func (j *DSGerrit) Validate() (err error) {
	j.URL = strings.TrimSpace(j.URL)
	if strings.HasSuffix(j.URL, "/") {
		j.URL = j.URL[:len(j.URL)-1]
	}
	ary := strings.Split(j.URL, "://")
	if len(ary) > 1 {
		j.URL = ary[1]
	}
	j.SSHKeyPath = os.ExpandEnv(j.SSHKeyPath)
	if j.SSHKeyPath == "" && j.SSHKey == "" {
		err = fmt.Errorf("Either SSH key or SSH key path must be set")
		return
	}
	if j.URL == "" || j.User == "" {
		err = fmt.Errorf("URL and user must be set")
	}
	return
}

// InitGerrit - initializes gerrit client
func (j *DSGerrit) InitGerrit(ctx *shared.Ctx) (err error) {
	if j.DisableHostKeyCheck {
		j.SSHOpts += "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
	}
	if j.SSHKey != "" {
		var f *os.File
		f, err = ioutil.TempFile("", "id_rsa")
		if err != nil {
			return
		}
		j.SSHKeyTempPath = f.Name()
		_, err = f.Write([]byte(j.SSHKey))
		if err != nil {
			return
		}
		err = f.Close()
		if err != nil {
			return
		}
		err = os.Chmod(j.SSHKeyTempPath, 0600)
		if err != nil {
			return
		}
		j.SSHOpts += "-i " + j.SSHKeyTempPath + " "
	} else {
		if j.SSHKeyPath != "" {
			j.SSHOpts += "-i " + j.SSHKeyPath + " "
		}
	}
	if strings.HasSuffix(j.SSHOpts, " ") {
		j.SSHOpts = j.SSHOpts[:len(j.SSHOpts)-1]
	}
	gerritCmd := fmt.Sprintf("ssh %s -p %d %s@%s gerrit", j.SSHOpts, j.SSHPort, j.User, j.URL)
	ary := strings.Split(gerritCmd, " ")
	for _, item := range ary {
		if item == "" {
			continue
		}
		j.GerritCmd = append(j.GerritCmd, item)
	}
	return
}

// GetGerritVersion - get gerrit version
func (j *DSGerrit) GetGerritVersion(ctx *shared.Ctx) (err error) {
	cmdLine := j.GerritCmd
	cmdLine = append(cmdLine, "version")
	var (
		sout string
		serr string
	)
	sout, serr, err = shared.ExecCommand(ctx, cmdLine, "", nil)
	if err != nil {
		shared.Printf("error executing %v: %v\n%s\n%s\n", cmdLine, err, sout, serr)
		return
	}
	match := GerritVersionRegexp.FindAllStringSubmatch(sout, -1)
	if len(match) < 1 {
		err = fmt.Errorf("cannot parse gerrit version '%s'", sout)
		return
	}
	j.VersionMajor, _ = strconv.Atoi(match[0][1])
	j.VersionMinor, _ = strconv.Atoi(match[0][2])
	if ctx.Debug > 0 {
		shared.Printf("Detected gerrit %d.%d\n", j.VersionMajor, j.VersionMinor)
	}
	return
}

// Init - initialize Gerrit data source
func (j *DSGerrit) Init(ctx *shared.Ctx) (err error) {
	shared.NoSSLVerify()
	ctx.InitEnv("Gerrit")
	j.AddFlags()
	ctx.Init()
	err = j.ParseArgs(ctx)
	if err != nil {
		return
	}
	err = j.Validate()
	if err != nil {
		return
	}
	if ctx.Debug > 1 {
		m := &models.Data{}
		shared.Printf("Gerrit: %+v\nshared context: %s\nModel: %+v", j, ctx.Info(), m)
	}
	return
}

// ItemID - return unique identifier for an item
func (j *DSGerrit) ItemID(item interface{}) string {
	id, ok := item.(map[string]interface{})["number"].(float64)
	if !ok {
		shared.Fatalf("gerrit: ItemID() - cannot extract number from %+v", shared.DumpKeys(item))
	}
	return fmt.Sprintf("%.0f", id)
}

// ItemUpdatedOn - return updated on date for an item
func (j *DSGerrit) ItemUpdatedOn(item interface{}) time.Time {
	epoch, ok := item.(map[string]interface{})["lastUpdated"].(float64)
	if !ok {
		shared.Fatalf("gerrit: ItemUpdatedOn() - cannot extract lastUpdated from %+v", shared.DumpKeys(item))
	}
	return time.Unix(int64(epoch), 0)
}

// GetGerritReviews - get gerrit reviews
func (j *DSGerrit) GetGerritReviews(ctx *shared.Ctx, after, before string, afterEpoch, beforeEpoch float64, startFrom int) (reviews []map[string]interface{}, newStartFrom int, err error) {
	cmdLine := j.GerritCmd
	// https://gerrit-review.googlesource.com/Documentation/user-search.html:
	// ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i ./ssh-key.secret -p XYZ usr@gerrit-url gerrit query after:'1970-01-01 00:00:00' limit: 2 (status:open OR status:closed) --all-approvals --all-reviewers --comments --format=JSON
	// For unknown reasons , gerrit is not returning data if number of seconds is not equal to 00 - so I'm updating query string to set seconds to ":00"
	after = after[:len(after)-3] + ":00"
	before = before[:len(before)-3] + ":00"
	cmdLine = append(cmdLine, "query")
	if ctx.ProjectFilter && ctx.Project != "" {
		cmdLine = append(cmdLine, "project:", ctx.Project)
	}
	cmdLine = append(cmdLine, `after:"`+after+`"`, `before:"`+before+`"`, "limit:", strconv.Itoa(j.MaxReviews), "(status:open OR status:closed)", "--all-approvals", "--all-reviewers", "--comments", "--format=JSON")
	// 2006-01-02[ 15:04:05[.890][ -0700]]
	if startFrom > 0 {
		cmdLine = append(cmdLine, "--start="+strconv.Itoa(startFrom))
	}
	var (
		sout string
		serr string
	)
	if ctx.Debug > 0 {
		shared.Printf("getting reviews via: %v\n", cmdLine)
	}
	sout, serr, err = shared.ExecCommand(ctx, cmdLine, "", nil)
	if err != nil {
		shared.Printf("error executing %v: %v\n%s\n%s\n", cmdLine, err, sout, serr)
		return
	}
	data := strings.Replace("["+strings.Replace(sout, "\n", ",", -1)+"]", ",]", "]", -1)
	var items []interface{}
	err = jsoniter.Unmarshal([]byte(data), &items)
	if err != nil {
		return
	}
	for i, iItem := range items {
		item, _ := iItem.(map[string]interface{})
		//Printf("#%d) %v\n", i, DumpKeys(item))
		iMoreChanges, ok := item["moreChanges"]
		if ok {
			moreChanges, ok := iMoreChanges.(bool)
			if ok {
				if moreChanges {
					newStartFrom = startFrom + i
					if ctx.Debug > 0 {
						shared.Printf("#%d) moreChanges: %v, newStartFrom: %d\n", i, moreChanges, newStartFrom)
					}
				}
			} else {
				shared.Printf("cannot read boolean value from %v\n", iMoreChanges)
			}
			return
		}
		_, ok = item["project"]
		if !ok {
			if ctx.Debug > 0 {
				shared.Printf("#%d) project not found: %+v", i, item)
			}
			continue
		}
		iLastUpdated, ok := item["lastUpdated"]
		if ok {
			lastUpdated, ok := iLastUpdated.(float64)
			if ok {
				if lastUpdated < afterEpoch || lastUpdated > beforeEpoch {
					if ctx.Debug > 1 {
						shared.Printf("#%d) lastUpdated: %v < afterEpoch: %v or > beforeEpoch: %v, skipping\n", i, lastUpdated, afterEpoch, beforeEpoch)
					}
					continue
				}
			} else {
				shared.Printf("cannot read float value from %v\n", iLastUpdated)
			}
		} else {
			shared.Printf("cannot read lastUpdated from %v\n", item)
		}
		reviews = append(reviews, item)
	}
	return
}

// GerritEnrichItems - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSGerrit) GerritEnrichItems(ctx *shared.Ctx, thrN int, items []interface{}, docs *[]interface{}, final bool) (err error) {
	// FIXME
	return
}

// AddMetadata - add metadata to the item
func (j *DSGerrit) AddMetadata(ctx *shared.Ctx, item interface{}) (mItem map[string]interface{}) {
	mItem = make(map[string]interface{})
	origin := j.URL
	tags := ctx.Tags
	if len(tags) == 0 {
		tags = []string{origin}
	}
	itemID := j.ItemID(item)
	updatedOn := j.ItemUpdatedOn(item)
	uuid := shared.UUIDNonEmpty(ctx, origin, itemID)
	timestamp := time.Now()
	mItem["backend_name"] = ctx.DS
	mItem["backend_version"] = GerritBackendVersion
	mItem["timestamp"] = fmt.Sprintf("%.06f", float64(timestamp.UnixNano())/1.0e9)
	mItem["uuid"] = uuid
	mItem["origin"] = origin
	mItem["tags"] = tags
	mItem["offset"] = float64(updatedOn.Unix())
	mItem["category"] = "review"
	mItem["search_fields"] = make(map[string]interface{})
	project, _ := shared.Dig(item, []string{"project"}, true, false)
	hash, _ := shared.Dig(item, []string{"id"}, true, false)
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", GerritDefaultSearchField}, itemID, false))
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", "project_name"}, project, false))
	shared.FatalOnError(shared.DeepSet(mItem, []string{"search_fields", "review_hash"}, hash, false))
	mItem["metadata__updated_on"] = shared.ToESDate(updatedOn)
	mItem["metadata__timestamp"] = shared.ToESDate(timestamp)
	// mItem[ProjectSlug] = ctx.ProjectSlug
	return
}

// Sync - sync Gerrit data source
func (j *DSGerrit) Sync(ctx *shared.Ctx) (err error) {
	thrN := shared.GetThreadsNum(ctx)
	if ctx.DateFrom != nil {
		shared.Printf("%s fetching from %v (%d threads)\n", j.URL, ctx.DateFrom, thrN)
	}
	if ctx.DateFrom == nil {
		ctx.DateFrom = shared.GetLastUpdate(ctx, j.URL)
		if ctx.DateFrom != nil {
			shared.Printf("%s resuming from %v (%d threads)\n", j.URL, ctx.DateFrom, thrN)
		}
	}
	if ctx.DateTo != nil {
		shared.Printf("%s fetching till %v (%d threads)\n", j.URL, ctx.DateTo, thrN)
	}
	// NOTE: Non-generic starts here
	err = j.InitGerrit(ctx)
	if err != nil {
		return
	}
	if j.SSHKeyTempPath != "" {
		defer func() {
			shared.Printf("removing temporary SSH key %s\n", j.SSHKeyTempPath)
			_ = os.Remove(j.SSHKeyTempPath)
		}()
	}
	// We don't have ancient gerrit versions like < 2.9 - this check is only for debugging
	if ctx.Debug > 1 {
		err = j.GetGerritVersion(ctx)
		if err != nil {
			return
		}
	}
	var (
		startFrom   int
		after       string
		afterEpoch  float64
		before      string
		beforeEpoch float64
	)
	if ctx.DateFrom != nil {
		after = shared.ToYMDHMSDate(*ctx.DateFrom)
		afterEpoch = float64(ctx.DateFrom.Unix())
	} else {
		after = "1970-01-01 00:00:00"
		afterEpoch = 0.0
	}
	if ctx.DateTo != nil {
		before = shared.ToYMDHMSDate(*ctx.DateTo)
		beforeEpoch = float64(ctx.DateTo.Unix())
	} else {
		before = "2100-01-01 00:00:00"
		beforeEpoch = 4102444800.0
	}
	var (
		ch            chan error
		allDocs       []interface{}
		allReviews    []interface{}
		allReviewsMtx *sync.Mutex
		escha         []chan error
		eschaMtx      *sync.Mutex
	)
	if thrN > 1 {
		ch = make(chan error)
		allReviewsMtx = &sync.Mutex{}
		eschaMtx = &sync.Mutex{}
	}
	nThreads := 0
	processReview := func(c chan error, review map[string]interface{}) (wch chan error, e error) {
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		esItem := j.AddMetadata(ctx, review)
		if ctx.Project != "" {
			review["project"] = ctx.Project
		}
		esItem["data"] = review
		if allReviewsMtx != nil {
			allReviewsMtx.Lock()
		}
		allReviews = append(allReviews, esItem)
		nReviews := len(allReviews)
		if nReviews >= ctx.PackSize {
			sendToQueue := func(c chan error) (ee error) {
				defer func() {
					if c != nil {
						c <- ee
					}
				}()
				ee = j.GerritEnrichItems(ctx, thrN, allReviews, &allDocs, false)
				// ee = SendToQueue(ctx, j, true, UUID, allReviews)
				if ee != nil {
					shared.Printf("error %v sending %d reviews to queue\n", ee, len(allReviews))
				}
				allReviews = []interface{}{}
				if allReviewsMtx != nil {
					allReviewsMtx.Unlock()
				}
				return
			}
			if thrN > 1 {
				wch = make(chan error)
				go func() {
					_ = sendToQueue(wch)
				}()
			} else {
				e = sendToQueue(nil)
				if e != nil {
					return
				}
			}
		} else {
			if allReviewsMtx != nil {
				allReviewsMtx.Unlock()
			}
		}
		return
	}
	if thrN > 1 {
		for {
			var reviews []map[string]interface{}
			reviews, startFrom, err = j.GetGerritReviews(ctx, after, before, afterEpoch, beforeEpoch, startFrom)
			if err != nil {
				return
			}
			for _, review := range reviews {
				go func(review map[string]interface{}) {
					var (
						e    error
						esch chan error
					)
					esch, e = processReview(ch, review)
					if e != nil {
						shared.Printf("process error: %v\n", e)
						return
					}
					if esch != nil {
						if eschaMtx != nil {
							eschaMtx.Lock()
						}
						escha = append(escha, esch)
						if eschaMtx != nil {
							eschaMtx.Unlock()
						}
					}
				}(review)
				nThreads++
				if nThreads == thrN {
					err = <-ch
					if err != nil {
						return
					}
					nThreads--
				}
			}
			if startFrom == 0 {
				break
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
	} else {
		for {
			var reviews []map[string]interface{}
			reviews, startFrom, err = j.GetGerritReviews(ctx, after, before, afterEpoch, beforeEpoch, startFrom)
			if err != nil {
				return
			}
			for _, review := range reviews {
				_, err = processReview(nil, review)
				if err != nil {
					return
				}
			}
			if startFrom == 0 {
				break
			}
		}
	}
	if eschaMtx != nil {
		eschaMtx.Lock()
	}
	for _, esch := range escha {
		err = <-esch
		if err != nil {
			if eschaMtx != nil {
				eschaMtx.Unlock()
			}
			return
		}
	}
	if eschaMtx != nil {
		eschaMtx.Unlock()
	}
	nReviews := len(allReviews)
	if ctx.Debug > 0 {
		shared.Printf("%d remaining reviews to send to queue\n", nReviews)
	}
	// NOTE: for all items, even if 0 - to flush the queue
	err = j.GerritEnrichItems(ctx, thrN, allReviews, &allDocs, true)
	//err = SendToQueue(ctx, j, true, UUID, allReviews)
	if err != nil {
		shared.Printf("Error %v sending %d reviews to queue\n", err, len(allReviews))
	}
	// NOTE: Non-generic ends here
	gMaxUpstreamDtMtx.Lock()
	defer gMaxUpstreamDtMtx.Unlock()
	shared.SetLastUpdate(ctx, j.URL, gMaxUpstreamDt)
	return
}

func main() {
	var (
		ctx    shared.Ctx
		gerrit DSGerrit
	)
	err := gerrit.Init(&ctx)
	if err != nil {
		shared.Printf("Error: %+v\n", err)
		return
	}
	err = gerrit.Sync(&ctx)
	if err != nil {
		shared.Printf("Error: %+v\n", err)
		return
	}
}
