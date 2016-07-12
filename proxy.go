package main

import (
	// Input/Output
	"bytes"
	"io"
	"io/ioutil"
	"log"
	"os"

	// Time
	"strconv"
	"time"

	// Webserver
	"net/http"

	"github.com/minio/minio-go"
)

var (
	// Loggers
	Info  *log.Logger
	Error *log.Logger

	minioRegion, minioBucket, minioAccessKey, minioSecretKey string

	// Health
	healthFile               string
	healthCheckCacheInterval int64
	lastHealthCheckTime      int64

	port string

	minioClient *Client
)

// Get an environment variable or use a default value if not set
func getEnvOrDefault(envName, defaultVal string, fatal bool) (envVal string) {
	envVal = os.Getenv(envName)
	if len(envVal) == 0 {
		if fatal {
			Error.Println("Unable to start as env " + envName + " is not defined")
			os.Exit(1)
		}
		envVal = defaultVal
		Info.Println("Using default " + envName + ": " + envVal)
	} else {
		Info.Println(envName + ": " + envVal)
	}
	return
}

// Get all the environment variables for this application
func getAllEnvVariables() {
	// Get the port that this webserver will be running upon
	port = getEnvOrDefault("PORT", "8000", false)

	// Get the AWS credentials
	minioRegion = getEnvOrDefault("MINIO_REGION", "us-east-1", false)
	minioBucket = getEnvOrDefault("MINIO_BUCKET", "test", true)
	minioAccessKey = getEnvOrDefault("MINIO_ACCESS_KEY_ID", "3XT91LA1P2T17IH2VHQX", true)
	minioSecretKey = getEnvOrDefault("MINIO_SECRET_ACCESS_KEY", "Zag9UH9P8sFjkEseTEYa+M2t8hq7VEmrWvkfuD+P", true)

	// Get the path for the healthFile and the time to cache
	healthFile = getEnvOrDefault("HEALTH_FILE", ".rest-minio-proxy", false)

	// Get the time to wait between health checks (we dont want to hammer S3)
	healthIntervalString := getEnvOrDefault("HEALTH_CACHE_INTERVAL", "120", false)
	healthIntervalInt, err := strconv.ParseInt(healthIntervalString, 10, 64)
	if err != nil {
		panic(err)
	}
	healthCheckCacheInterval = healthIntervalInt

}

// Serve a health request
func serveHealth(w http.ResponseWriter, r *http.Request) {
	// Ensure that we can connect to the S3 bucket provided (every 10 seconds max)
	currentTime := time.Now().Unix()

	if (currentTime - lastHealthCheckTime) > healthCheckCacheInterval {
		Info.Println("Making health check for path '" + healthFile + "'")

		// Check that we have read permissions on the status file (we may not have listing permissions)
		//params := &s3.GetObjectInput{Bucket: aws.String(awsBucket), Key: aws.String(healthFile)}
		//_, err := s3Session.GetObject(params)

		_, err := minioClient.GetObject(minioBucket, healthFile)

		if handleHTTPException(healthFile, w, err) != nil {
			Error.Println("Health check failed")
			return
		}

		Info.Println("Health check passed")
		lastHealthCheckTime = currentTime
	}
	io.WriteString(w, "OK")
}

func handleHTTPException(path string, w http.ResponseWriter, err error) (e error) {
	if err != nil {
		// golang error
		http.Error(w, "An internal error occurred: "+err.Error(), http.StatusInternalServerError)
	}
	return err
}

// Initialise loggers
func initLogging(infoHandle io.Writer, errorHandle io.Writer) {
	Info = log.New(infoHandle, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
	Error = log.New(errorHandle, "ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}

// Main method
func main() {
	initLogging(os.Stdout, os.Stderr)

	// Reset health check status
	lastHealthCheckTime = 0

	// Set up all the environment variables
	getAllEnvVariables()

	// Set up the S3 connection
	//	s3Session = s3.New(session.New(), &aws.Config{Region: aws.String(awsRegion)})
	minioClient, err := minio.New("minio:9000", minioAccessKey, minioSecretKey, ssl)
	if err != nil {
		fmt.Println(err)
		return
	}

	Info.Println("Startup complete")

	// Run the webserver
	http.HandleFunc("/", serveMinioFile)
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		Error.Println("ListenAndServe: ", err)
		os.Exit(1)
	}

}

func serveMinioFile(w http.ResponseWriter, r *http.Request) {
	var method = r.Method
	var path = r.URL.Path[1:] // Remove the / from the start of the URL

	// A file with no path cannot be served
	if path == "" {
		http.Error(w, "Path must be provided", http.StatusBadRequest)
		return
	}

	// Ensure the health endpoint is honoured
	if path == "healthz" {
		if method == "GET" {
			serveHealth(w, r)
		} else {
			http.Error(w, "/healthz is restricted to GET requests", http.StatusMethodNotAllowed)
		}
		return
	}

	Info.Println("Handling " + method + " request for '" + path + "'")

	switch method {
	case "GET":
		serveGetMinioFile(path, w, r)
	/*case "PUT":
		servePutS3File(path, w, r)
	case "DELETE":
		serveDeleteS3File(path, w, r)
	*/
	default:
		http.Error(w, "Method "+method+" not supported", http.StatusMethodNotAllowed)
	}
}

func serveGetMinioFile(filePath string, w http.ResponseWriter, r *http.Request) {
	/*params := &s3.GetObjectInput{Bucket: aws.String(awsBucket), Key: aws.String(filePath)}
	resp, err := s3Session.GetObject(params)
	*/

	object, err := minioClient.GetObject("mybucket", "photo.jpg")
	if err != nil {
		fmt.Println(err)
		return
	}

	if handleHTTPException(filePath, w, err) != nil {
		return
	}

	// File is ready to download
	io.Copy(w, object)
}
