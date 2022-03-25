package main

import (
	"encoding/base64"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/LF-Engineering/lfx-event-schema/service"
	"github.com/LF-Engineering/lfx-event-schema/service/insights"
	"github.com/LF-Engineering/lfx-event-schema/service/insights/gerrit"
	"github.com/LF-Engineering/lfx-event-schema/service/repository"
	"github.com/LF-Engineering/lfx-event-schema/service/user"
	"github.com/LF-Engineering/lfx-event-schema/utils/datalake"

	shared "github.com/LF-Engineering/insights-datasource-shared"
	"github.com/LF-Engineering/insights-datasource-shared/cryptography"
	elastic "github.com/LF-Engineering/insights-datasource-shared/elastic"
	logger "github.com/LF-Engineering/insights-datasource-shared/ingestjob"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	jsoniter "github.com/json-iterator/go"
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
	// GerritDataSource - constant for gerrit source
	GerritDataSource = "gerrit"
	// GerritDefaultStream - Stream To Publish reviews
	GerritDefaultStream = "PUT-S3-gerrit"
	// GerritConnector ...
	GerritConnector = "gerrit-connector"
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
)

// Publisher - for streaming data to Kinesis
type Publisher interface {
	PushEvents(action, source, eventType, subEventType, env string, data []interface{}) error
}

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
	FlagStream              *string
	// Non-config variables
	SSHOpts        string   // SSH Options
	SSHKeyTempPath string   // if used SSHKey - temp file with this name was used to store key contents
	GerritCmd      []string // gerrit remote command used to fetch data
	VersionMajor   int      // gerrit major version
	VersionMinor   int      // gerrit minor version
	// Publisher & stream
	Publisher
	Stream string // stream to publish the data
	Logger logger.Logger
}

// AddPublisher - sets Kinesis publisher
func (j *DSGerrit) AddPublisher(publisher Publisher) {
	j.Publisher = publisher
}

// PublisherPushEvents - this is a fake function to test publisher locally
// FIXME: don't use when done implementing
func (j *DSGerrit) PublisherPushEvents(ev, ori, src, cat, env string, v []interface{}) error {
	data, err := jsoniter.Marshal(v)
	shared.Printf("publish[ev=%s ori=%s src=%s cat=%s env=%s]: %d items: %+v -> %v\n", ev, ori, src, cat, env, len(v), string(data), err)
	return nil
}

// AddLogger - adds logger
func (j *DSGerrit) AddLogger(ctx *shared.Ctx) {
	client, err := elastic.NewClientProvider(&elastic.Params{
		URL:      os.Getenv("ELASTIC_LOG_URL"),
		Password: os.Getenv("ELASTIC_LOG_PASSWORD"),
		Username: os.Getenv("ELASTIC_LOG_USER"),
	})
	if err != nil {
		shared.Printf("AddLogger error: %+v", err)
		return
	}
	logProvider, err := logger.NewLogger(client, os.Getenv("STAGE"))
	if err != nil {
		shared.Printf("AddLogger error: %+v", err)
		return
	}
	j.Logger = *logProvider
}

// WriteLog - writes to log
func (j *DSGerrit) WriteLog(ctx *shared.Ctx, timestamp time.Time, status, message string) {
	_ = j.Logger.Write(&logger.Log{
		Connector: GerritDataSource,
		Configuration: []map[string]string{
			{
				"GERRIT_URL":     j.URL,
				"GERRIT_PROJECT": ctx.Project,
				"ProjectSlug":    ctx.Project,
			}},
		Status:    status,
		CreatedAt: timestamp,
		Message:   message,
	})
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
	j.FlagStream = flag.String("gerrit-stream", GerritDefaultStream, "gerrit kinesis stream name, for example PUT-S3-gerrit")
}

// ParseArgs - parse gerrit specific environment variables
func (j *DSGerrit) ParseArgs(ctx *shared.Ctx) (err error) {
	// Cryptography
	encrypt, err := cryptography.NewEncryptionClient()
	if err != nil {
		return err
	}

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
		j.User, err = encrypt.Decrypt(j.User)
		if err != nil {
			return err
		}
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
		encodedKey, err := encrypt.Decrypt(*j.FlagSSHKey)
		if err != nil {
			return err
		}
		decodedKey, err := base64.StdEncoding.DecodeString(encodedKey)
		if err != nil {
			return err
		}
		j.SSHKey = string(decodedKey)
	} else {
		if ctx.EnvSet("SSH_KEY") {
			encodedKey, err := encrypt.Decrypt(ctx.Env("SSH_KEY"))
			if err != nil {
				return err
			}
			decodedKey, err := base64.StdEncoding.DecodeString(encodedKey)
			if err != nil {
				return err
			}
			j.SSHKey = string(decodedKey)
		}
	}

	// shared.Printf("user=%s, key=%s\n", j.User, j.SSHKey)
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

	// gerrit Kinesis stream
	j.Stream = GerritDefaultStream
	if shared.FlagPassed(ctx, "stream") {
		j.Stream = *j.FlagStream
	}
	if ctx.EnvSet("STREAM") {
		j.Stream = ctx.Env("STREAM")
	}
	// gGerritDataSource.Categories = j.Categories
	// gGerritMetaData.Project = ctx.Project
	// gGerritMetaData.Tags = ctx.Tags
	return
}

// Validate - is current DS configuration OK?
func (j *DSGerrit) Validate(ctx *shared.Ctx) (err error) {
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
		return
	}
	if ctx.Project == "<GERRIT-PROJECT>" {
		err = fmt.Errorf("Project cannot be %s", ctx.Project)
		return
	}
	if ctx.Project == "" && ctx.ProjectFilter {
		err = fmt.Errorf("Project cannot be empty when requesting internal project filtering")
		return
	}
	if ctx.Project != "" && !ctx.ProjectFilter {
		err = fmt.Errorf(
			"When you specify project, you also need to request internal project filtering, " +
				"setting project without filtering was only allowed in V1 to process all projects " +
				"but set a custom project name on all of them",
		)
	}
	return
}

// InitGerrit - initializes gerrit client
func (j *DSGerrit) InitGerrit(ctx *shared.Ctx) (err error) {
	if j.DisableHostKeyCheck {
		j.SSHOpts += "-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
	}
	path := ""
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
		path = j.SSHKeyTempPath
	} else {
		if j.SSHKeyPath != "" {
			j.SSHOpts += "-i " + j.SSHKeyPath + " "
		}
		path = j.SSHKeyPath
	}
	if ctx.Debug > 1 {
		content, err := ioutil.ReadFile(path)
		fmt.Printf("File contents(path=%s,err=%v): %s\n", path, err, content)
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
	err = j.Validate(ctx)
	if err != nil {
		return
	}
	if ctx.Debug > 1 {
		g := &gerrit.Changeset{}
		shared.Printf("Gerrit: %+v\nshared context: %s\nModel: %+v\n", j, ctx.Info(), g)
	}
	if ctx.Debug > 0 {
		shared.Printf("stream: '%s'\n", j.Stream)
	}
	if j.Stream != "" {
		sess, err := session.NewSession()
		if err != nil {
			return err
		}
		s3Client := s3.New(sess)
		objectStore := datalake.NewS3ObjectStore(s3Client)
		datalakeClient := datalake.NewStoreClient(&objectStore)
		j.AddPublisher(&datalakeClient)
	}
	j.AddLogger(ctx)
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
	// https://gerrit-review.googlesource.com/Documentation/cmd-query.html
	// ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i ./ssh-key.secret -p XYZ usr@gerrit-url gerrit query after:'1970-01-01 00:00:00' limit: 2 (status:open OR status:closed) --all-approvals --all-reviewers --comments --format=JSON
	// For unknown reasons , gerrit is not returning data if number of seconds is not equal to 00 - so I'm updating query string to set seconds to ":00"
	after = after[:len(after)-3] + ":00"
	before = before[:len(before)-3] + ":00"
	cmdLine = append(cmdLine, "query")
	if ctx.ProjectFilter && ctx.Project != "" {
		cmdLine = append(cmdLine, "project:", ctx.Project)
	}
	cmdLine = append(
		cmdLine,
		`after:"`+after+`"`,
		`before:"`+before+`"`,
		"limit:", strconv.Itoa(j.MaxReviews),
		"(status:open OR status:closed)",
		"--all-approvals",
		"--all-reviewers",
		"--comments",
		"--patch-sets",
		"--files",
		"--commit-message",
		"--dependencies",
		"--submit-records",
		"--format=JSON",
	)
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
		// shared.Printf("#%d) %v\n", i, DumpKeys(item))
		// shared.Printf("#%d) %v\n", i, shared.AsJSON(item))
		// shared.Printf("#%d) %v\n", i, shared.PrettyPrint(item))
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

// IdentityForObject - construct identity from a given object
func (j *DSGerrit) IdentityForObject(ctx *shared.Ctx, obj map[string]interface{}) (identity [3]string) {
	if ctx.Debug > 2 {
		defer func() {
			shared.Printf("%+v -> %+v\n", obj, identity)
		}()
	}
	item := obj
	data, ok := shared.Dig(item, []string{"data"}, false, true)
	if ok {
		mp, ok := data.(map[string]interface{})
		if ok {
			if ctx.Debug > 2 {
				shared.Printf("digged in data: %+v\n", obj)
			}
			item = mp
		}
	}
	for i, prop := range []string{"name", "username", "email"} {
		iVal, ok := shared.Dig(item, []string{prop}, false, true)
		if ok {
			val, ok := iVal.(string)
			if ok {
				identity[i] = val
			}
		} else {
			identity[i] = ""
		}
	}
	return
}

// GetRoleIdentity - return identity data for a given role
func (j *DSGerrit) GetRoleIdentity(ctx *shared.Ctx, item map[string]interface{}, role string) (identity map[string]interface{}) {
	iRole, ok := shared.Dig(item, []string{role}, false, true)
	if ok {
		roleObj, ok := iRole.(map[string]interface{})
		if ok {
			ident := j.IdentityForObject(ctx, roleObj)
			identity = map[string]interface{}{
				"name":     ident[0],
				"username": ident[1],
				"email":    ident[2],
				"role":     role,
			}
		}
	}
	return
}

// GetRoles - return identities for given roles
func (j *DSGerrit) GetRoles(ctx *shared.Ctx, item map[string]interface{}, roles []string, dt time.Time) (identities []map[string]interface{}) {
	for _, role := range roles {
		identity := j.GetRoleIdentity(ctx, item, role)
		if identity == nil || len(identity) == 0 {
			continue
		}
		identity["dt"] = dt
		identities = append(identities, identity)
	}
	return
}

// ConvertDates - convert floating point dates to datetimes
func (j *DSGerrit) ConvertDates(ctx *shared.Ctx, review map[string]interface{}) {
	for _, field := range []string{"timestamp", "createdOn", "lastUpdated"} {
		idt, ok := shared.Dig(review, []string{field}, false, true)
		if !ok {
			continue
		}
		fdt, ok := idt.(float64)
		if !ok {
			continue
		}
		review[field] = time.Unix(int64(fdt), 0)
		// Printf("converted %s: %v -> %v\n", field, idt, review[field])
	}
	iPatchSets, ok := shared.Dig(review, []string{"patchSets"}, false, true)
	if ok {
		patchSets, ok := iPatchSets.([]interface{})
		if ok {
			for patchNum, iPatch := range patchSets {
				patch, ok := iPatch.(map[string]interface{})
				if !ok {
					continue
				}
				var patchTS time.Time
				fNumber, _ := patch["number"].(float64)
				number := fmt.Sprintf("%.0f", fNumber)
				ref, _ := patch["ref"].(string)
				patchsetSID := number + ":" + ref
				field := "createdOn"
				idt, ok := shared.Dig(patch, []string{field}, false, true)
				if ok {
					fdt, ok := idt.(float64)
					if ok {
						patchTS = time.Unix(int64(fdt), 0)
						patch[field] = patchTS
						// Printf("converted patch %s: %v -> %v\n", field, idt, patch[field])
					}
				}
				iApprovals, ok := shared.Dig(patch, []string{"approvals"}, false, true)
				if ok {
					approvals, ok := iApprovals.([]interface{})
					if ok {
						for _, iApproval := range approvals {
							approval, ok := iApproval.(map[string]interface{})
							if !ok {
								continue
							}
							field := "grantedOn"
							idt, ok := shared.Dig(approval, []string{field}, false, true)
							if ok {
								fdt, ok := idt.(float64)
								if ok {
									approval[field] = time.Unix(int64(fdt), 0)
									// Printf("converted patch approval %s: %v -> %v\n", field, idt, approval[field])
								}
							}
						}
					}
				}
				if patchTS.IsZero() {
					continue
				}
				iComments, ok := shared.Dig(patch, []string{"comments"}, false, true)
				if ok {
					comments, ok := iComments.([]interface{})
					if ok {
						for commentNum, iComment := range comments {
							comment, ok := iComment.(map[string]interface{})
							if !ok {
								continue
							}
							comment["level"] = "patchset"
							comment["patchset_comment_index"] = fmt.Sprintf("%d:%d", patchNum, commentNum)
							comment["patchset_sid"] = patchsetSID
							// patchSet level comments have no timestamp field, we use patchSet's creation date
							comment["timestamp"] = patchTS
						}
					}
				}
			}
		}
	}
	iComments, ok := shared.Dig(review, []string{"comments"}, false, true)
	if ok {
		comments, ok := iComments.([]interface{})
		if ok {
			for _, iComment := range comments {
				comment, ok := iComment.(map[string]interface{})
				if !ok {
					continue
				}
				comment["level"] = "changeset"
				field := "timestamp"
				idt, ok := shared.Dig(comment, []string{field}, false, true)
				if ok {
					fdt, ok := idt.(float64)
					if ok {
						comment[field] = time.Unix(int64(fdt), 0)
						// Printf("converted comment %s: %v -> %v\n", field, idt, comment[field])
					}
				}
			}
		}
	}
}

// LastChangesetApprovalValue - return last approval status
func (j *DSGerrit) LastChangesetApprovalValue(ctx *shared.Ctx, patchSets []interface{}) (status interface{}) {
	if ctx.Debug > 2 {
		defer func() {
			shared.Printf("LastChangesetApprovalValue: %+v -> %+v\n", patchSets, status)
		}()
	}
	nPatchSets := len(patchSets)
	if ctx.Debug > 2 {
		shared.Printf("LastChangesetApprovalValue: %d patch sets\n", nPatchSets)
	}
	for i := nPatchSets - 1; i >= 0; i-- {
		iPatchSet := patchSets[i]
		patchSet, ok := iPatchSet.(map[string]interface{})
		if !ok {
			continue
		}
		iApprovals, ok := patchSet["approvals"]
		if !ok {
			if ctx.Debug > 2 {
				shared.Printf("LastChangesetApprovalValue: no approvals\n")
			}
			continue
		}
		approvals, ok := iApprovals.([]interface{})
		if !ok {
			continue
		}
		authorUsername, okAuthorUsername := shared.Dig(patchSet, []string{"author", "username"}, false, true)
		authorEmail, okAuthorEmail := shared.Dig(patchSet, []string{"author", "email"}, false, true)
		if authorUsername == "" {
			okAuthorUsername = false
		}
		if authorEmail == "" {
			okAuthorEmail = false
		}
		nApprovals := len(approvals)
		if ctx.Debug > 2 {
			shared.Printf("LastChangesetApprovalValue: %d approvals\n", nApprovals)
		}
		for j := nApprovals - 1; j >= 0; j-- {
			iApproval := approvals[j]
			approval, ok := iApproval.(map[string]interface{})
			if !ok {
				continue
			}
			iApprovalType, ok := approval["type"]
			if !ok {
				continue
			}
			approvalType, ok := iApprovalType.(string)
			if !ok || approvalType != GerritCodeReviewApprovalType {
				if ctx.Debug > 2 {
					shared.Printf("LastChangesetApprovalValue: incorrect type %+v\n", iApprovalType)
				}
				continue
			}
			byUsername, okByUsername := shared.Dig(approval, []string{"by", "username"}, false, true)
			byEmail, okByEmail := shared.Dig(approval, []string{"by", "email"}, false, true)
			if byUsername == "" {
				okByUsername = false
			}
			if byEmail == "" {
				okByEmail = false
			}
			// Printf("LastChangesetApprovalValue: (%s,%s,%s,%s) (%v,%v,%v,%v)\n", authorUsername, authorEmail, byUsername, byEmail, okAuthorUsername, okAuthorEmail, okByUsername, okByEmail)
			var okStatus bool
			if okByUsername && okAuthorUsername {
				// Printf("LastChangesetApprovalValue: usernames set\n")
				byUName, _ := byUsername.(string)
				authorUName, _ := authorUsername.(string)
				if byUName != authorUName {
					status, okStatus = approval["value"]
				}
			} else if okByEmail && okAuthorEmail {
				// Printf("LastChangesetApprovalValue: emails set\n")
				byMail, _ := byEmail.(string)
				authorMail, _ := authorEmail.(string)
				if byMail != authorMail {
					status, okStatus = approval["value"]
				}
			} else {
				// Printf("LastChangesetApprovalValue: else case\n")
				status, okStatus = approval["value"]
			}
			if ctx.Debug > 2 {
				shared.Printf("LastChangesetApprovalValue: final (%+v,%+v)\n", status, okStatus)
			}
			if okStatus && status != nil {
				return
			}
		}
	}
	return
}

// EnrichItem - return rich item from raw item
func (j *DSGerrit) EnrichItem(ctx *shared.Ctx, item map[string]interface{}) (rich map[string]interface{}, err error) {
	rich = make(map[string]interface{})
	if ctx.Debug > 1 {
		defer func() {
			fmt.Printf("raw = %s\n", shared.PrettyPrint(item))
			fmt.Printf("rich = %s\n", shared.PrettyPrint(rich))
		}()
	}
	for _, field := range shared.RawFields {
		v, _ := item[field]
		rich[field] = v
	}
	iUpdatedOn, _ := shared.Dig(item, []string{"metadata__updated_on"}, true, false)
	var updatedOn time.Time
	updatedOn, err = shared.TimeParseInterfaceString(iUpdatedOn)
	if err != nil {
		return
	}
	review, ok := item["data"].(map[string]interface{})
	if !ok {
		err = fmt.Errorf("missing data field in item %+v", shared.DumpPreview(item, 100))
		return
	}
	j.ConvertDates(ctx, review)
	iReviewStatus, ok := review["status"]
	var reviewStatus string
	if ok {
		reviewStatus, _ = iReviewStatus.(string)
	}
	rich["status"] = reviewStatus
	rich["branch"], _ = review["branch"]
	rich["url"], _ = review["url"]
	rich["githash"], _ = review["id"]
	var createdOn time.Time
	iCreatedOn, ok := review["createdOn"]
	if ok {
		createdOn, _ = iCreatedOn.(time.Time)
	}
	rich["opened"] = createdOn
	rich["repository"], _ = review["project"]
	rich["repo_short_name"], _ = rich["repository"]
	rich["changeset_number"], _ = review["number"]
	uuid, ok := rich["uuid"].(string)
	if !ok {
		err = fmt.Errorf("cannot read string uuid from %+v", shared.DumpPreview(rich, 100))
		return
	}
	changesetNumber := j.ItemID(review)
	rich["id"] = uuid + "_changeset_" + changesetNumber
	summary := ""
	iSummary, ok := review["subject"]
	if ok {
		summary, _ = iSummary.(string)
	}
	rich["summary_analyzed"] = summary
	if len(summary) > shared.KeywordMaxlength {
		summary = summary[:shared.KeywordMaxlength]
	}
	rich["summary"] = summary
	commitMessage, ok := review["commitMessage"].(string)
	commitBody := ""
	if ok {
		ary := strings.Split(commitMessage, "\n")
		lines := []string{}
		for _, line := range ary {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			lines = append(lines, line)
		}
		if len(lines) > 1 {
			commitBody = strings.Join(lines[1:], "\n")
		}
	}
	rich["commit_message"] = commitMessage
	rich["commit_body"] = commitBody
	rich["name"] = nil
	rich["domain"] = nil
	ownerName, ok := shared.Dig(review, []string{"owner", "name"}, false, true)
	if ok {
		rich["name"] = ownerName
		iOwnerEmail, ok := shared.Dig(review, []string{"owner", "email"}, false, true)
		if ok {
			ownerEmail, ok := iOwnerEmail.(string)
			if ok {
				ary := strings.Split(ownerEmail, "@")
				if len(ary) > 1 {
					rich["domain"] = strings.TrimSpace(ary[1])
				}
			}
		}
	}
	iPatchSets, ok := shared.Dig(review, []string{"patchSets"}, false, true)
	nPatchSets := 0
	var patchSets []interface{}
	if ok {
		patchSets, ok = iPatchSets.([]interface{})
		if ok {
			nPatchSets = len(patchSets)
			firstPatch, ok := patchSets[0].(map[string]interface{})
			if ok {
				iCreatedOn, ok = firstPatch["createdOn"]
				if ok {
					createdOn, _ = iCreatedOn.(time.Time)
				}
			}
		}
	}
	rich["created_on"] = createdOn
	rich["patchsets"] = nPatchSets
	status := j.LastChangesetApprovalValue(ctx, patchSets)
	rich["status_value"] = status
	rich["changeset_status_value"] = status
	rich["changeset_status"] = reviewStatus
	var lastUpdatedOn time.Time
	iLastUpdatedOn, ok := review["lastUpdated"]
	if ok {
		lastUpdatedOn, _ = iLastUpdatedOn.(time.Time)
	}
	rich["last_updated"] = lastUpdatedOn
	if reviewStatus == "MERGED" {
		rich["timeopen"] = float64(lastUpdatedOn.Sub(createdOn).Seconds()) / 86400.0
		rich["closed"] = updatedOn
		rich["merged"] = updatedOn
	} else if reviewStatus == "ABANDONED" {
		rich["closed"] = updatedOn
	} else {
		rich["timeopen"] = float64(time.Now().Sub(createdOn).Seconds()) / 86400.0
	}
	wip, ok := shared.Dig(review, []string{"wip"}, false, true)
	if ok {
		rich["wip"] = wip
	} else {
		rich["wip"] = false
	}
	rich["open"], _ = shared.Dig(review, []string{"open"}, false, true)
	rich["type"] = "changeset"
	rich["metadata__updated_on"] = updatedOn
	rich["roles"] = j.GetRoles(ctx, review, GerritReviewRoles, updatedOn)
	// NOTE: From shared
	rich["metadata__enriched_on"] = time.Now()
	// rich[ProjectSlug] = ctx.ProjectSlug
	// rich["groups"] = ctx.Groups
	return
}

// EnrichApprovals - return rich items from raw approvals
func (j *DSGerrit) EnrichApprovals(ctx *shared.Ctx, review, patchSet map[string]interface{}, approvals []map[string]interface{}) (richItems []interface{}, err error) {
	iPatchSetID, ok := patchSet["id"]
	if !ok {
		err = fmt.Errorf("cannot get id property of patchset: %+v", patchSet)
		return
	}
	patchSetID, ok := iPatchSetID.(string)
	if !ok {
		err = fmt.Errorf("cannot get string id property of patchset: %+v", iPatchSetID)
		return
	}
	copyFields := []string{"wip", "open", "url", "summary", "repository", "branch", "changeset_number", "changeset_status", "changeset_status_value", "patchset_number", "patchset_revision", "patchset_ref", "repo_short_name"}
	for _, approval := range approvals {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := patchSet[field]
			rich[field] = v
		}
		for _, field := range copyFields {
			rich[field] = patchSet[field]
		}
		rich["approval_author_name"] = nil
		rich["approval_author_domain"] = nil
		authorName, ok := shared.Dig(approval, []string{"by", "name"}, false, true)
		if ok {
			rich["approval_author_name"] = authorName
			iAuthorEmail, ok := shared.Dig(approval, []string{"by", "email"}, false, true)
			if ok {
				authorEmail, ok := iAuthorEmail.(string)
				if ok {
					ary := strings.Split(authorEmail, "@")
					if len(ary) > 1 {
						rich["approval_author_domain"] = strings.TrimSpace(ary[1])
					}
				}
			}
		}
		//
		var created time.Time
		iCreated, ok := approval["grantedOn"]
		if ok {
			created, ok = iCreated.(time.Time)
		}
		if !ok {
			err = fmt.Errorf("cannot read grantedOn property from approval: %+v", approval)
			return
		}
		rich["approval_granted_on"] = created
		rich["approval_value"], _ = approval["value"]
		rich["approval_type"], _ = approval["type"]
		desc := ""
		iDesc, ok := approval["description"]
		if ok {
			desc, _ = iDesc.(string)
		}
		rich["approval_description_analyzed"] = desc
		if len(desc) > shared.KeywordMaxlength {
			desc = desc[:shared.KeywordMaxlength]
		}
		rich["approval_description"] = desc
		rich["type"] = "approval"
		rich["id"] = patchSetID + "_approval_" + fmt.Sprintf("%d", created.Unix())
		rich["changeset_created_on"], _ = review["created_on"]
		rich["metadata__updated_on"] = created
		rich["roles"] = j.GetRoles(ctx, approval, GerritApprovalRoles, created)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		// rich[ProjectSlug] = ctx.ProjectSlug
		// rich["groups"] = ctx.Groups
		richItems = append(richItems, rich)
	}
	return
}

// EnrichPatchsets - return rich items from raw patch sets
func (j *DSGerrit) EnrichPatchsets(ctx *shared.Ctx, review map[string]interface{}, patchSets []map[string]interface{}) (richItems []interface{}, err error) {
	copyFields := []string{"wip", "open", "url", "summary", "repository", "branch", "changeset_number", "changeset_status", "changeset_status_value", "repo_short_name"}
	iReviewID, ok := review["id"]
	if !ok {
		err = fmt.Errorf("cannot get id property of review: %+v", review)
		return
	}
	reviewID, ok := iReviewID.(string)
	if !ok {
		err = fmt.Errorf("cannot get string id property of review: %+v", iReviewID)
		return
	}
	for _, patchSet := range patchSets {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := review[field]
			rich[field] = v
		}
		for _, field := range copyFields {
			rich[field] = review[field]
		}
		rich["patchset_author_name"] = nil
		rich["patchset_author_domain"] = nil
		authorName, ok := shared.Dig(patchSet, []string{"author", "name"}, false, true)
		if ok {
			rich["patchset_author_name"] = authorName
			iAuthorEmail, ok := shared.Dig(patchSet, []string{"author", "email"}, false, true)
			if ok {
				authorEmail, ok := iAuthorEmail.(string)
				if ok {
					ary := strings.Split(authorEmail, "@")
					if len(ary) > 1 {
						rich["patchset_author_domain"] = strings.TrimSpace(ary[1])
					}
				}
			}
		}
		rich["patchset_uploader_name"] = nil
		rich["patchset_uploader_domain"] = nil
		uploaderName, ok := shared.Dig(patchSet, []string{"uploader", "name"}, false, true)
		if ok {
			rich["patchset_uploader_name"] = uploaderName
			iUploaderEmail, ok := shared.Dig(patchSet, []string{"uploader", "email"}, false, true)
			if ok {
				uploaderEmail, ok := iUploaderEmail.(string)
				if ok {
					ary := strings.Split(uploaderEmail, "@")
					if len(ary) > 1 {
						rich["patchset_uploader_domain"] = strings.TrimSpace(ary[1])
					}
				}
			}
		}
		var created time.Time
		iCreated, ok := patchSet["createdOn"]
		if ok {
			created, ok = iCreated.(time.Time)
		}
		if !ok {
			err = fmt.Errorf("cannot read createdOn property from patchSet: %+v", patchSet)
			return
		}
		rich["patchset_created_on"] = created
		number := patchSet["number"]
		rich["patchset_number"] = number
		rich["patchset_isDraft"], _ = patchSet["isDraft"]
		rich["patchset_kind"], _ = patchSet["kind"]
		rich["patchset_ref"], _ = patchSet["ref"]
		rich["patchset_revision"], _ = patchSet["revision"]
		rich["patchset_sizeDeletions"], _ = patchSet["sizeDeletions"]
		rich["patchset_sizeInsertions"], _ = patchSet["sizeInsertions"]
		rich["type"] = "patchset"
		rich["id"] = reviewID + "_patchset_" + fmt.Sprintf("%v", number)
		rich["metadata__updated_on"] = created
		rich["roles"] = j.GetRoles(ctx, patchSet, GerritPatchsetRoles, created)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		// rich[ProjectSlug] = ctx.ProjectSlug
		// rich["groups"] = ctx.Groups
		richItems = append(richItems, rich)
		iApprovals, ok := shared.Dig(patchSet, []string{"approvals"}, false, true)
		if ok {
			approvalsAry, ok := iApprovals.([]interface{})
			if ok {
				var approvals []map[string]interface{}
				for _, iApproval := range approvalsAry {
					approval, ok := iApproval.(map[string]interface{})
					if !ok {
						continue
					}
					approvals = append(approvals, approval)
				}
				if len(approvals) > 0 {
					var riches []interface{}
					riches, err = j.EnrichApprovals(ctx, review, rich, approvals)
					if err != nil {
						return
					}
					richItems = append(richItems, riches...)
				}
			}
		}
	}
	return
}

// EnrichComments - return rich items from raw patch sets
func (j *DSGerrit) EnrichComments(ctx *shared.Ctx, review map[string]interface{}, comments []map[string]interface{}) (richItems []interface{}, err error) {
	copyFields := []string{"wip", "open", "url", "summary", "repository", "branch", "changeset_number", "repo_short_name"}
	iReviewID, ok := review["id"]
	if !ok {
		err = fmt.Errorf("cannot get id property of review: %+v", review)
		return
	}
	reviewID, ok := iReviewID.(string)
	if !ok {
		err = fmt.Errorf("cannot get string id property of review: %+v", iReviewID)
		return
	}
	for _, comment := range comments {
		rich := make(map[string]interface{})
		for _, field := range shared.RawFields {
			v, _ := review[field]
			rich[field] = v
		}
		for _, field := range copyFields {
			rich[field] = review[field]
		}
		rich["reviewer_name"] = nil
		rich["reviewer_domain"] = nil
		reviewerName, ok := shared.Dig(comment, []string{"reviewer", "name"}, false, true)
		if ok {
			rich["reviewer_name"] = reviewerName
			iReviewerEmail, ok := shared.Dig(comment, []string{"reviewer", "email"}, false, true)
			if ok {
				reviewerEmail, ok := iReviewerEmail.(string)
				if ok {
					ary := strings.Split(reviewerEmail, "@")
					if len(ary) > 1 {
						rich["reviewer_domain"] = strings.TrimSpace(ary[1])
					}
				}
			}
		}
		var created time.Time
		iCreated, ok := comment["timestamp"]
		if ok {
			created, ok = iCreated.(time.Time)
		}
		if !ok {
			err = fmt.Errorf("cannot read timestamp property from comment: %+v", comment)
			return
		}
		rich["comment_created_on"] = created
		message := ""
		iMessage, ok := comment["message"]
		if ok {
			message, _ = iMessage.(string)
		}
		rich["comment_message_analyzed"] = message
		if len(message) > shared.KeywordMaxlength {
			message = message[:shared.KeywordMaxlength]
		}
		rich["comment_message"] = message
		rich["patchset_sid"], _ = comment["patchset_sid"]
		level, _ := comment["level"]
		rich["level"] = level
		rich["type"] = "comment"
		if level == "patchset" {
			patchsetCommentNum, _ := comment["patchset_comment_index"]
			rich["patchset_comment_index"] = patchsetCommentNum
			rich["id"] = reviewID + "_comment_" + fmt.Sprintf("%s_%d", patchsetCommentNum, created.Unix())
		} else {
			rich["id"] = reviewID + "_comment_" + fmt.Sprintf("%d", created.Unix())
		}
		rich["metadata__updated_on"] = created
		rich["roles"] = j.GetRoles(ctx, comment, GerritCommentRoles, created)
		// NOTE: From shared
		rich["metadata__enriched_on"] = time.Now()
		richItems = append(richItems, rich)
	}
	return
}

// GetProjectRepoURL - return gerrit repository URL for a given project
func (j *DSGerrit) GetProjectRepoURL(project string) string {
	// FIXME: based on Fayaz comment, we probably need to catch more cases in different Gerrit instances
	if !strings.Contains(j.URL, "://") {
		return "https://" + j.URL + "/r/admin/repos/" + project
	}
	return j.URL + "/r/admin/repos/" + project
}

// GetModelData - return data in lfx-event-schema format
func (j *DSGerrit) GetModelData(ctx *shared.Ctx, docs []interface{}) (data map[string][]interface{}, err error) {
	data = make(map[string][]interface{})
	defer func() {
		if err != nil {
			return
		}
		changesetBaseEvent := gerrit.ChangesetBaseEvent{
			Connector:        insights.GerritConnector,
			ConnectorVersion: GerritBackendVersion,
			Source:           insights.GerritSource,
		}
		changesetCommentBaseEvent := gerrit.ChangesetCommentBaseEvent{
			Connector:        insights.GerritConnector,
			ConnectorVersion: GerritBackendVersion,
			Source:           insights.GerritSource,
		}
		patchsetCommentBaseEvent := gerrit.PatchsetCommentBaseEvent{
			Connector:        insights.GerritConnector,
			ConnectorVersion: GerritBackendVersion,
			Source:           insights.GerritSource,
		}
		approvalBaseEvent := gerrit.ApprovalBaseEvent{
			Connector:        insights.GerritConnector,
			ConnectorVersion: GerritBackendVersion,
			Source:           insights.GerritSource,
		}
		patchsetBaseEvent := gerrit.PatchsetBaseEvent{
			Connector:        insights.GerritConnector,
			ConnectorVersion: GerritBackendVersion,
			Source:           insights.GerritSource,
		}
		for k, v := range data {
			switch k {
			case "changeset_created":
				baseEvent := service.BaseEvent{
					Type: service.EventType(gerrit.ChangesetCreatedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GerritConnector,
						UpdatedBy: GerritConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, changeset := range v {
					ary = append(ary, gerrit.ChangesetCreatedEvent{
						ChangesetBaseEvent: changesetBaseEvent,
						BaseEvent:          baseEvent,
						Payload:            changeset.(gerrit.Changeset),
					})
				}
				data[k] = ary
			case "changeset_merged":
				baseEvent := service.BaseEvent{
					Type: service.EventType(gerrit.ChangesetMergedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GerritConnector,
						UpdatedBy: GerritConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, changeset := range v {
					ary = append(ary, gerrit.ChangesetMergedEvent{
						ChangesetBaseEvent: changesetBaseEvent,
						BaseEvent:          baseEvent,
						Payload:            changeset.(gerrit.Changeset),
					})
				}
				data[k] = ary
			case "changeset_closed":
				baseEvent := service.BaseEvent{
					Type: service.EventType(gerrit.ChangesetClosedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GerritConnector,
						UpdatedBy: GerritConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, changeset := range v {
					ary = append(ary, gerrit.ChangesetClosedEvent{
						ChangesetBaseEvent: changesetBaseEvent,
						BaseEvent:          baseEvent,
						Payload:            changeset.(gerrit.Changeset),
					})
				}
				data[k] = ary
			case "changeset_comment_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(gerrit.ChangesetCommentAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GerritConnector,
						UpdatedBy: GerritConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, changesetComment := range v {
					ary = append(ary, gerrit.ChangesetCommentAddedEvent{
						ChangesetCommentBaseEvent: changesetCommentBaseEvent,
						BaseEvent:                 baseEvent,
						Payload:                   changesetComment.(gerrit.ChangesetComment),
					})
				}
				data[k] = ary
			case "patchset_comment_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(gerrit.PatchsetCommentAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GerritConnector,
						UpdatedBy: GerritConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, patchsetComment := range v {
					ary = append(ary, gerrit.PatchsetCommentAddedEvent{
						PatchsetCommentBaseEvent: patchsetCommentBaseEvent,
						BaseEvent:                baseEvent,
						Payload:                  patchsetComment.(gerrit.PatchsetComment),
					})
				}
				data[k] = ary
			case "approval_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(gerrit.ApprovalAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GerritConnector,
						UpdatedBy: GerritConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, approval := range v {
					ary = append(ary, gerrit.ApprovalAddedEvent{
						ApprovalBaseEvent: approvalBaseEvent,
						BaseEvent:         baseEvent,
						Payload:           approval.(gerrit.Approval),
					})
				}
				data[k] = ary
			case "patchset_added":
				baseEvent := service.BaseEvent{
					Type: service.EventType(gerrit.PatchsetAddedEvent{}.Event()),
					CRUDInfo: service.CRUDInfo{
						CreatedBy: GerritConnector,
						UpdatedBy: GerritConnector,
						CreatedAt: time.Now().Unix(),
						UpdatedAt: time.Now().Unix(),
					},
				}
				ary := []interface{}{}
				for _, patchset := range v {
					ary = append(ary, gerrit.PatchsetAddedEvent{
						PatchsetBaseEvent: patchsetBaseEvent,
						BaseEvent:         baseEvent,
						Payload:           patchset.(gerrit.Patchset),
					})
				}
				data[k] = ary
			default:
				err = fmt.Errorf("unknown changeset '%s' event", k)
				return
			}
		}
	}()
	changesetID, repoID, userID, patchsetID, approvalID, commentID, patchID := "", "", "", "", "", "", ""
	source := GerritDataSource
	for _, iDoc := range docs {
		doc, _ := iDoc.(map[string]interface{})
		csetRepo, _ := doc["repository"].(string)
		csetHash, _ := doc["githash"].(string)
		csetNumber, _ := doc["changeset_number"].(float64)
		sCsetNumber := fmt.Sprintf("%.0f", csetNumber)
		sIID := sCsetNumber + ":" + csetHash
		repoID, err = repository.GenerateRepositoryID(csetRepo, j.URL, GerritDataSource)
		// shared.Printf("GenerateRepositoryID(%s,%s,%s) -> %s\n", csetRepo, j.URL, GerritDataSource, repoID)
		if err != nil {
			shared.Printf("GenerateRepositoryID(%s,%s,%s): %+v for %+v\n", csetRepo, j.URL, GerritDataSource, err, doc)
			return
		}
		changesetID, err = gerrit.GenerateGerritChangesetID(repoID, sIID)
		// shared.Printf("gerrit.GenerateGerritChangesetID(%s,%s) -> %s\n", repoID, sIID, changesetID)
		if err != nil {
			shared.Printf("gerrit.GenerateGerritChangesetID(%s,%s): %+v for %+v\n", repoID, sIID, err, doc)
			return
		}
		csetURL, _ := doc["url"].(string)
		repoURL := j.GetProjectRepoURL(csetRepo)
		csetSummary, _ := doc["summary"].(string)
		csetBody, _ := doc["commit_body"].(string)
		csetStatus, _ := doc["changeset_status"].(string)
		lowerStatus := strings.ToLower(csetStatus)
		lines := strings.Split(csetSummary, "\n")
		title := lines[0]
		createdOn, _ := doc["opened"].(time.Time)
		updatedOn, _ := doc["metadata__updated_on"].(time.Time)
		closedOn, isClosed := doc["closed"].(time.Time)
		mergedOn, isMerged := doc["merged"].(time.Time)
		contributors := []insights.Contributor{}
		// Changeset owner (author) starts
		roles, okRoles := doc["roles"].([]map[string]interface{})
		if okRoles {
			for _, role := range roles {
				name, _ := role["name"].(string)
				username, _ := role["username"].(string)
				email, _ := role["email"].(string)
				// No identity data postprocessing in V2
				// name, username = shared.PostprocessNameUsername(name, username, email)
				userID, err = user.GenerateIdentity(&source, &email, &name, &username)
				if err != nil {
					shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
					return
				}
				contributor := insights.Contributor{
					Role:   insights.AuthorRole,
					Weight: 1.0,
					Identity: user.UserIdentityObjectBase{
						ID:         userID,
						Email:      email,
						IsVerified: false,
						Name:       name,
						Username:   username,
						Source:     source,
					},
				}
				contributors = append(contributors, contributor)
			}
		}
		// Changeset owner (author) ends
		// Patchsets and approvals start
		objAry, okObj := doc["patchset_array"].([]interface{})
		if okObj {
			for _, iObj := range objAry {
				obj, okObj := iObj.(map[string]interface{})
				if !okObj || obj == nil {
					continue
				}
				objType, _ := obj["type"].(string)
				roles, okRoles := obj["roles"].([]map[string]interface{})
				if objType == "patchset" {
					// patchset start
					sha, _ := obj["patchset_revision"].(string)
					if !okRoles || sha == "" || len(roles) == 0 {
						continue
					}
					patchsetCreatedOn, _ := obj["patchset_created_on"].(time.Time)
					if patchsetCreatedOn.After(updatedOn) {
						updatedOn = patchsetCreatedOn
					}
					patchsetContributors := []insights.Contributor{}
					for _, role := range roles {
						roleType, _ := role["role"].(string)
						name, _ := role["name"].(string)
						username, _ := role["username"].(string)
						email, _ := role["email"].(string)
						// No identity data postprocessing in V2
						// name, username = shared.PostprocessNameUsername(name, username, email)
						// possible roles: author, uploader
						roleValue := insights.AuthorRole
						if roleType == "uploader" {
							// FIXME: V1 mapped "uploader" as a "committer" - is this OK or should we create insights.UploaderRole? LG: for me this is OK.
							roleValue = insights.CommitterRole
						}
						userID, err = user.GenerateIdentity(&source, &email, &name, &username)
						if err != nil {
							shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
							return
						}
						contributor := insights.Contributor{
							Role:   roleValue,
							Weight: 1.0,
							Identity: user.UserIdentityObjectBase{
								ID:         userID,
								Email:      email,
								IsVerified: false,
								Name:       name,
								Username:   username,
								Source:     source,
							},
						}
						patchsetContributors = append(patchsetContributors, contributor)
						// If we want to add patchset contributors to changeset object
						// contributors = append(contributors, contributor)
					}
					fNumber, _ := obj["patchset_number"].(float64)
					number := fmt.Sprintf("%.0f", fNumber)
					ref, _ := obj["patchset_ref"].(string)
					patchsetSID := number + ":" + ref
					patchsetID, err = gerrit.GenerateGerritPatchsetID(changesetID, patchsetSID)
					// shared.Printf("gerrit.GenerateGerritPatchsetID(%s,%s) -> %s\n", changesetID, patchsetSID, patchsetID)
					if err != nil {
						shared.Printf("gerrit.GenerateGerritPatchsetID(%s,%s): %+v for %+v\n", changesetID, patchsetSID, err, doc)
						return
					}
					patchset := gerrit.Patchset{
						ID:         patchsetID,
						PatchsetID: patchsetSID,
						// ChangesetID:     sIID,
						ChangesetID:     changesetID,
						CommitSHA:       sha,
						Contributors:    shared.DedupContributors(patchsetContributors),
						SyncTimestamp:   time.Now(),
						SourceTimestamp: patchsetCreatedOn,
						// FIXME we don't have anything more useful, patchset "obj" also has "summary" but this is also a copy from the parent changeset
						Body: csetSummary,
					}
					key := "patchset_added"
					ary, ok := data[key]
					if !ok {
						ary = []interface{}{patchset}
					} else {
						ary = append(ary, patchset)
					}
					data[key] = ary
					// patchset end
				} else {
					// approval start
					sReviewBody, _ := obj["approval_description"].(string)
					//sCommitID, _ := obj["patchset_revision"].(string)
					sReviewState, _ := obj["approval_value"].(string)
					reviewState := int64(0)
					if sReviewState != "" {
						var e error
						reviewState, e = strconv.ParseInt(sReviewState, 10, 64)
						if e != nil {
							shared.Printf("WARNING: invalid review state: '%s' in %+v, assuming value 0\n", sReviewState, obj)
						}
					} else {
						shared.Printf("WARNING: empty review state in %+v, assuming value 0\n", obj)
					}
					reviewCreatedOn, _ := obj["approval_granted_on"].(time.Time)
					if reviewCreatedOn.After(updatedOn) {
						updatedOn = reviewCreatedOn
					}
					isApproved := reviewState >= 0
					approvalSID, _ := obj["id"].(string)
					// We need to calculate patsetID for an approval
					fNumber, _ := obj["patchset_number"].(float64)
					number := fmt.Sprintf("%.0f", fNumber)
					ref, _ := obj["patchset_ref"].(string)
					patchsetSID := number + ":" + ref
					patchsetID, err = gerrit.GenerateGerritPatchsetID(changesetID, patchsetSID)
					// shared.Printf("in approval gerrit.GenerateGerritPatchsetID(%s,%s) -> %s\n", changesetID, patchsetSID, patchsetID)
					if err != nil {
						shared.Printf("gerrit.GenerateGerritPatchsetID(%s,%s): %+v for %+v\n", changesetID, patchsetSID, err, doc)
						return
					}
					for _, role := range roles {
						roleType, _ := role["role"].(string)
						if roleType != "by" {
							continue
						}
						name, _ := role["name"].(string)
						username, _ := role["username"].(string)
						email, _ := role["email"].(string)
						// No identity data postprocessing in V2
						// name, username = shared.PostprocessNameUsername(name, username, email)
						userID, err = user.GenerateIdentity(&source, &email, &name, &username)
						if err != nil {
							shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
							return
						}
						role := insights.ReviewerRole
						if isApproved {
							role = insights.ApproverRole
						}
						contributor := insights.Contributor{
							Role:   role,
							Weight: 1.0,
							Identity: user.UserIdentityObjectBase{
								ID:         userID,
								Email:      email,
								IsVerified: false,
								Name:       name,
								Username:   username,
								Source:     source,
							},
						}
						approvalID, err = gerrit.GenerateGerritApprovalID(patchsetID, approvalSID)
						// shared.Printf("gerrit.GenerateGerritApprovalID(%s,%s) -> %s\n", patchsetID, approvalSID, approvalID)
						if err != nil {
							shared.Printf("gerrit.GenerateGerritApprovalID(%s,%s): %+v for %+v\n", patchsetID, approvalSID, err, doc)
							return
						}
						// If we want to add approver as a contributor on the changeset object
						// contributors = append(contributors, contributor)
						approval := gerrit.Approval{
							ID:              approvalID,
							PatchsetID:      patchsetID,
							ApprovalID:      approvalSID,
							Body:            sReviewBody,
							Contributor:     contributor,
							State:           fmt.Sprintf("%d", reviewState),
							SyncTimestamp:   time.Now(),
							SourceTimestamp: reviewCreatedOn,
						}
						key := "approval_added"
						ary, ok := data[key]
						if !ok {
							ary = []interface{}{approval}
						} else {
							ary = append(ary, approval)
						}
						data[key] = ary
						// approval end
					}
				}
			}
		}
		// patchsets and approvals end
		// comments start
		commentsAry, okComments := doc["comments_array"].([]interface{})
		if okComments {
			for _, iComment := range commentsAry {
				comment, okComment := iComment.(map[string]interface{})
				if !okComment || comment == nil {
					continue
				}
				roles, okRoles := comment["roles"].([]map[string]interface{})
				if !okRoles || len(roles) == 0 {
					continue
				}
				level, _ := comment["level"].(string)
				sCommentBody, _ := comment["comment_message"].(string)
				commentCreatedOn, _ := comment["comment_created_on"].(time.Time)
				sCommentID, _ := comment["id"].(string)
				if commentCreatedOn.After(updatedOn) {
					updatedOn = commentCreatedOn
				}
				for _, role := range roles {
					roleType, _ := role["role"].(string)
					if roleType != "reviewer" {
						continue
					}
					name, _ := role["name"].(string)
					username, _ := role["username"].(string)
					email, _ := role["email"].(string)
					// No identity data postprocessing in V2
					//name, username = shared.PostprocessNameUsername(name, username, email)
					userID, err = user.GenerateIdentity(&source, &email, &name, &username)
					if err != nil {
						shared.Printf("GenerateIdentity(%s,%s,%s,%s): %+v for %+v\n", source, email, name, username, err, doc)
						return
					}
					contributor := insights.Contributor{
						Role:   insights.CommenterRole,
						Weight: 1.0,
						Identity: user.UserIdentityObjectBase{
							ID:         userID,
							Email:      email,
							IsVerified: false,
							Name:       name,
							Username:   username,
							Source:     source,
						},
					}
					if level == "changeset" {
						commentID, err = gerrit.GenerateGerritChangesetCommentID(repoID, sCommentID)
						// shared.Printf("gerrit.GenerateGerritChangesetCommentID(%s,%s) -> %s\n", repoID, sCommentID, commentID)
						if err != nil {
							shared.Printf("gerrit.GenerateGerritChangesetCommentID(%s,%s): %+v for %+v\n", repoID, sCommentID, err, doc)
							return
						}
						// If we want to add comments as a contributor on the changeset object
						// contributors = append(contributors, contributor)
						comment := gerrit.ChangesetComment{
							ID:          commentID,
							ChangesetID: changesetID,
							Comment: insights.Comment{
								Body: sCommentBody,
								// FIXME: we don't have anything else
								CommentURL:      csetURL,
								SourceTimestamp: commentCreatedOn,
								SyncTimestamp:   time.Now(),
								CommentID:       sCommentID,
								Contributor:     contributor,
								Orphaned:        false,
							},
						}
						key := "changeset_comment_added"
						ary, ok := data[key]
						if !ok {
							ary = []interface{}{comment}
						} else {
							ary = append(ary, comment)
						}
						data[key] = ary
					} else {
						commentID, err = gerrit.GenerateGerritPatchsetCommentID(repoID, sCommentID)
						// shared.Printf("gerrit.GenerateGerritPatchsetCommentID(%s,%s) -> %s\n", repoID, sCommentID, commentID)
						if err != nil {
							shared.Printf("gerrit.GenerateGerritPatchsetCommentID(%s,%s): %+v for %+v\n", repoID, sCommentID, err, doc)
							return
						}
						patchsetSID, _ := comment["patchset_sid"].(string)
						patchID, err = gerrit.GenerateGerritPatchsetID(changesetID, patchsetSID)
						// shared.Printf("in approval gerrit.GenerateGerritPatchsetID(%s,%s) -> %s\n", changesetID, patchsetSID, patchID)
						if err != nil {
							shared.Printf("gerrit.GenerateGerritPatchsetID(%s,%s): %+v for %+v\n", changesetID, patchsetSID, err, doc)
							return
						}
						// If we want to add comments as a contributor on the changeset object
						// contributors = append(contributors, contributor)
						comment := gerrit.PatchsetComment{
							ID:         commentID,
							PatchsetID: patchID,
							Comment: insights.Comment{
								Body: sCommentBody,
								// FIXME: we don't have anything else
								CommentURL:      csetURL,
								SourceTimestamp: commentCreatedOn,
								SyncTimestamp:   time.Now(),
								CommentID:       sCommentID,
								Contributor:     contributor,
								Orphaned:        false,
							},
						}
						key := "patchset_comment_added"
						ary, ok := data[key]
						if !ok {
							ary = []interface{}{comment}
						} else {
							ary = append(ary, comment)
						}
						data[key] = ary
					}
				}
			}
		}
		// comments end
		// shared.Printf("(repo,repourl,cseturl,summary,siid,closed,merged)=('%s','%s','%s','%s','%s',(%v,%v),(%v,%v))\n", csetRepo, repoURL, csetURL, csetSummary, sIID, isClosed, closedOn, isMerged, mergedOn)
		// Final changeset object
		changeset := gerrit.Changeset{
			ID:            changesetID,
			RepositoryID:  repoID,
			RepositoryURL: repoURL,
			Contributors:  shared.DedupContributors(contributors),
			ChangeRequest: insights.ChangeRequest{
				Title:            title,
				Body:             csetBody,
				ChangeRequestID:  sIID,
				ChangeRequestURL: csetURL,
				State:            insights.ChangeRequestState(lowerStatus),
				SyncTimestamp:    time.Now(),
				SourceTimestamp:  createdOn,
				Orphaned:         false,
			},
		}
		key := "changeset_created"
		ary, ok := data[key]
		if !ok {
			ary = []interface{}{changeset}
		} else {
			ary = append(ary, changeset)
		}
		data[key] = ary
		// Fake merge "event"
		if isMerged {
			// changeset.Contributors = []insights.Contributor{}
			changeset.SyncTimestamp = time.Now()
			changeset.SourceTimestamp = mergedOn
			key := "changeset_merged"
			ary, ok := data[key]
			if !ok {
				ary = []interface{}{changeset}
			} else {
				ary = append(ary, changeset)
			}
			data[key] = ary
		}
		// Fake "close" event (not merged and closed)
		if isClosed && !isMerged {
			// changeset.Contributors = []insights.Contributor{}
			changeset.SyncTimestamp = time.Now()
			changeset.SourceTimestamp = closedOn
			key := "changeset_closed"
			ary, ok := data[key]
			if !ok {
				ary = []interface{}{changeset}
			} else {
				ary = append(ary, changeset)
			}
			data[key] = ary
		}
		gMaxUpstreamDtMtx.Lock()
		if updatedOn.After(gMaxUpstreamDt) {
			gMaxUpstreamDt = updatedOn
		}
		gMaxUpstreamDtMtx.Unlock()
	}
	return
}

// GerritEnrichItems - iterate items and enrich them
// items is a current pack of input items
// docs is a pointer to where extracted identities will be stored
func (j *DSGerrit) GerritEnrichItems(ctx *shared.Ctx, thrN int, items []interface{}, docs *[]interface{}, final bool) (err error) {
	shared.Printf("input processing(%d/%d/%v)\n", len(items), len(*docs), final)
	outputDocs := func() {
		if len(*docs) > 0 {
			// actual output
			shared.Printf("output processing(%d/%d/%v)\n", len(items), len(*docs), final)
			var (
				reviewsData map[string][]interface{}
				jsonBytes   []byte
				err         error
			)
			reviewsData, err = j.GetModelData(ctx, *docs)
			if err == nil {
				if j.Publisher != nil {
					insightsStr := "insights"
					reviewsStr := "reviews"
					envStr := os.Getenv("STAGE")
					for k, v := range reviewsData {
						switch k {
						case "changeset_created":
							ev, _ := v[0].(gerrit.ChangesetCreatedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GerritDataSource, reviewsStr, envStr, v)
						case "changeset_merged":
							ev, _ := v[0].(gerrit.ChangesetMergedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GerritDataSource, reviewsStr, envStr, v)
						case "changeset_closed":
							ev, _ := v[0].(gerrit.ChangesetClosedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GerritDataSource, reviewsStr, envStr, v)
						case "changeset_comment_added":
							ev, _ := v[0].(gerrit.ChangesetCommentAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GerritDataSource, reviewsStr, envStr, v)
						case "patchset_comment_added":
							ev, _ := v[0].(gerrit.PatchsetCommentAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GerritDataSource, reviewsStr, envStr, v)
						case "approval_added":
							ev, _ := v[0].(gerrit.ApprovalAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GerritDataSource, reviewsStr, envStr, v)
						case "patchset_added":
							ev, _ := v[0].(gerrit.PatchsetAddedEvent)
							err = j.Publisher.PushEvents(ev.Event(), insightsStr, GerritDataSource, reviewsStr, envStr, v)
						default:
							err = fmt.Errorf("unknown event type '%s'", k)
						}
						if err != nil {
							break
						}
					}
				} else {
					jsonBytes, err = jsoniter.Marshal(reviewsData)
				}
			}
			if err != nil {
				shared.Printf("Error: %+v\n", err)
				return
			}
			if j.Publisher == nil {
				shared.Printf("%s\n", string(jsonBytes))
			}
			*docs = []interface{}{}
			gMaxUpstreamDtMtx.Lock()
			defer gMaxUpstreamDtMtx.Unlock()
			shared.SetLastUpdate(ctx, j.URL, gMaxUpstreamDt)
		}
	}
	if final {
		defer func() {
			outputDocs()
		}()
	}
	// NOTE: non-generic code starts
	if ctx.Debug > 0 {
		shared.Printf("gerrit enrich items %d/%d func\n", len(items), len(*docs))
	}
	var (
		mtx *sync.RWMutex
		ch  chan error
	)
	if thrN > 1 {
		mtx = &sync.RWMutex{}
		ch = make(chan error)
	}
	getRichItem := func(doc map[string]interface{}) (rich map[string]interface{}, e error) {
		rich, e = j.EnrichItem(ctx, doc)
		if e != nil {
			return
		}
		data, _ := shared.Dig(doc, []string{"data"}, true, false)
		iPatchSets, ok := shared.Dig(data, []string{"patchSets"}, false, true)
		var patchComms []map[string]interface{}
		if ok {
			patchSets, ok := iPatchSets.([]interface{})
			if ok {
				var patches []map[string]interface{}
				for _, iPatch := range patchSets {
					patch, ok := iPatch.(map[string]interface{})
					if !ok {
						continue
					}
					patches = append(patches, patch)
					iComments, ok := shared.Dig(patch, []string{"comments"}, false, true)
					if ok {
						comments, ok := iComments.([]interface{})
						if ok {
							for _, iComment := range comments {
								comment, ok := iComment.(map[string]interface{})
								if !ok {
									continue
								}
								patchComms = append(patchComms, comment)
							}
						}
					}
				}
				if len(patches) > 0 {
					var riches []interface{}
					riches, e = j.EnrichPatchsets(ctx, rich, patches)
					if e != nil {
						return
					}
					rich["patchset_array"] = riches
				}
			}
		}
		iComments, ok := shared.Dig(data, []string{"comments"}, false, true)
		if ok {
			comments, ok := iComments.([]interface{})
			if ok {
				var comms []map[string]interface{}
				for _, iComment := range comments {
					comment, ok := iComment.(map[string]interface{})
					if !ok {
						continue
					}
					comms = append(comms, comment)
				}
				if len(patchComms) > 0 {
					for _, comm := range patchComms {
						comms = append(comms, comm)
					}
				}
				if len(comms) > 0 {
					var riches []interface{}
					riches, e = j.EnrichComments(ctx, rich, comms)
					if e != nil {
						return
					}
					rich["comments_array"] = riches
				}
			}
		}
		return
	}
	nThreads := 0
	procItem := func(c chan error, idx int) (e error) {
		if thrN > 1 {
			mtx.RLock()
		}
		item := items[idx]
		if thrN > 1 {
			mtx.RUnlock()
		}
		defer func() {
			if c != nil {
				c <- e
			}
		}()
		// NOTE: never refer to _source - we no longer use ES
		doc, ok := item.(map[string]interface{})
		if !ok {
			e = fmt.Errorf("Failed to parse document %+v", doc)
			return
		}
		rich, e := getRichItem(doc)
		if e != nil {
			return
		}
		if thrN > 1 {
			mtx.Lock()
		}
		*docs = append(*docs, rich)
		// NOTE: flush here
		if len(*docs) >= ctx.PackSize {
			outputDocs()
		}
		if thrN > 1 {
			mtx.Unlock()
		}
		return
	}
	if thrN > 1 {
		for i := range items {
			go func(i int) {
				_ = procItem(ch, i)
			}(i)
			nThreads++
			if nThreads == thrN {
				err = <-ch
				if err != nil {
					return
				}
				nThreads--
			}
		}
		for nThreads > 0 {
			err = <-ch
			nThreads--
			if err != nil {
				return
			}
		}
		return
	}
	for i := range items {
		err = procItem(nil, i)
		if err != nil {
			return
		}
	}
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
		cleanup := func() {
			if ctx.Debug > 0 {
				shared.Printf("removing temporary SSH key %s\n", j.SSHKeyTempPath)
			}
			_ = os.Remove(j.SSHKeyTempPath)
		}
		defer cleanup()
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-c
			cleanup()
			os.Exit(1)
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
	timestamp := time.Now()
	shared.SetSyncMode(true, false)
	shared.SetLogLoggerError(false)
	shared.AddLogger(&gerrit.Logger, GerritDataSource, logger.Internal, []map[string]string{{"GERRIT_URL": gerrit.URL, "GERRIT_PROJECT": ctx.Project, "ProjectSlug": ctx.Project}})
	gerrit.WriteLog(&ctx, timestamp, logger.InProgress, "")
	err = gerrit.Sync(&ctx)
	if err != nil {
		shared.Printf("Error: %+v\n", err)
		gerrit.WriteLog(&ctx, timestamp, logger.Failed, err.Error())
		return
	}
	gerrit.WriteLog(&ctx, timestamp, logger.Done, "")
}
