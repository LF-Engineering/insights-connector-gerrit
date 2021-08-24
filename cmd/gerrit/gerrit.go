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
	/*
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
					reviews, startFrom, err = j.GetGerritReviews(ctx, after, afterEpoch, before, beforeEpoch, startFrom)
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
					reviews, startFrom, err = j.GetGerritReviews(ctx, after, afterEpoch, before, beforeEpoch, startFrom)
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
	*/
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
