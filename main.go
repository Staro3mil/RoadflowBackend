package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/dgrijalva/jwt-go"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"

	//"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"golang.org/x/crypto/bcrypt"
)

const secretKey = "my_secret_key"

var (
	mongoClient    *mongo.Client
	userCollection *mongo.Collection
)

// global S3 client and bucket name
var (
	s3Client   *s3.Client
	bucketName string
	region     string
)

// User represents the user model stored in MongoDB.
type User struct {
	Email        string `bson:"email" json:"email"`
	Name         string `bson:"name" json:"name"`
	Password     string `bson:"password" json:"password"`
	CaptchaToken string `bson:"captchaToken" json:"captchaToken"`
}

// recaptchaResponse mirrors Google’s JSON response structure.
type recaptchaResponse struct {
	Success     bool     `json:"success"`
	Score       float64  `json:"score,omitempty"`
	Action      string   `json:"action,omitempty"`
	ChallengeTs string   `json:"challenge_ts,omitempty"`
	Hostname    string   `json:"hostname,omitempty"`
	ErrorCodes  []string `json:"error-codes,omitempty"`
}

func getMongoClient() (*mongo.Client, error) {
	if mongoClient != nil {
		return mongoClient, nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	log.Println("Connecting to MongoDB...")
	uri := os.Getenv("MONGO_URI")
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(uri))
	if err != nil {
		return nil, err
	}
	if err := client.Ping(ctx, nil); err != nil {
		return nil, err
	}
	mongoClient = client // cache for future calls
	return mongoClient, nil
}

// func connectMongo() *mongo.Client {

// 	// Load .env file
// 	err := godotenv.Load()
// 	if err != nil {
// 		log.Fatal("Error loading .env file")
// 	}

// 	// Use your MongoDB URI.
// 	mongoURI := os.Getenv("MONGO_URI")
// 	clientOptions := options.Client().ApplyURI(mongoURI)
// 	client, err := mongo.Connect(context.Background(), clientOptions)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	// Ping the database to ensure a successful connection
// 	// Send a ping to confirm a successful connection
// 	if err := client.Database("go_app").RunCommand(context.TODO(), bson.D{{"ping", 1}}).Err(); err != nil {
// 		panic(err)
// 	}
// 	log.Println("Connected to MongoDB!")
// 	return client
// }

func initS3() {
	// Load AWS creds & region from environment or ~/.aws/*
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("unable to load AWS SDK config, %v", err)
	}
	s3Client = s3.NewFromConfig(cfg)

	// read bucket & region from env
	bucketName = os.Getenv("S3_BUCKET")
	region = os.Getenv("AWS_REGION")
	fmt.Println("AWS:", bucketName)
	fmt.Println("AWS_REGION:", region)
	if bucketName == "" || region == "" {
		log.Fatal("S3_BUCKET and AWS_REGION must be set")
	}
}

func register(c *gin.Context) {
	client, err := getMongoClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "DB unavailable"})
		return
	}
	userCollection = client.Database("go_app").Collection("user")

	var input User
	var existingUser User
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Insert the user document into MongoDB with a timeout context
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	err = userCollection.FindOne(ctx, bson.M{"email": input.Email}).Decode(&existingUser)
	if err == nil {
		// If no error, then a user was found.
		c.JSON(http.StatusBadRequest, gin.H{"error": "Email already in use"})
		return
	} else if err != mongo.ErrNoDocuments {
		// If the error is not ErrNoDocuments, then something else went wrong
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Server error"})
		return
	}

	_, err = verifyCaptcha(input.CaptchaToken)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid CAPTCHA"})
		return
	}

	hashedPassword, err := bcrypt.GenerateFromPassword([]byte(input.Password), bcrypt.DefaultCost)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Server error"})
		return
	}
	input.Password = string(hashedPassword)

	defer cancel()
	_, err = userCollection.InsertOne(ctx, input)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Could not register user"})
		return
	}

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"email": input.Email,
		"name":  input.Name,
		"exp":   time.Now().Add(time.Hour * 2).Unix(), // Token expiration time
	})

	// Sign and get the complete encoded token as a string
	tokenString, err := token.SignedString([]byte(secretKey))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
		return
	}

	// Return a personalized greeting after successful login.
	c.JSON(http.StatusOK, gin.H{"message": "Hello " + input.Name, "token": tokenString})
}

// verifyCaptcha sends the token to Google's v3 verify endpoint.
func verifyCaptcha(token string) (*recaptchaResponse, error) {
	// Build the form-encoded POST
	form := url.Values{
		"secret":   {os.Getenv("RECAPTCHA_SECRET")},
		"response": {token},
		// If you want, you can also pass "remoteip": {userIPString}
	}
	resp, err := http.PostForm("https://www.google.com/recaptcha/api/siteverify", form)
	if err != nil {
		log.Printf("[reCAPTCHA] HTTP POST error: %v\n", err)
		return nil, err
	}
	defer resp.Body.Close()

	// Read the raw body for extra debugging (optional)
	rawBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Printf("[reCAPTCHA] Error reading response body: %v\n", err)
		return nil, err
	}

	// Unmarshal into our struct
	var result recaptchaResponse
	if err := json.Unmarshal(rawBody, &result); err != nil {
		log.Printf("[reCAPTCHA] JSON unmarshal error: %v\nFull body: %s\n", err, string(rawBody))
		return nil, err
	}

	// Log the entire response so you see error codes, score, etc.
	log.Printf("[reCAPTCHA] siteverify response: %+v\n", result)

	return &result, nil
}

func login(c *gin.Context) {
	client, err := getMongoClient()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "DB unavailable"})
		return
	}
	userCollection = client.Database("go_app").Collection("user")

	var input User
	if err := c.ShouldBindJSON(&input); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Query MongoDB for the user by email
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var user User
	err = userCollection.FindOne(ctx, bson.M{"email": input.Email}).Decode(&user)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid credentials"})
		return
	}
	_, err = verifyCaptcha(input.CaptchaToken)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid CAPTCHA"})
		return
	}

	// Compare the provided password with the hashed password stored in MongoDB
	if err := bcrypt.CompareHashAndPassword([]byte(user.Password), []byte(input.Password)); err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid credentials"})
		return
	}

	// Create a new token object, specifying signing method and the claims
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"email": user.Email,
		"name":  user.Name,
		"exp":   time.Now().Add(time.Hour * 2).Unix(), // Token expiration time
	})

	fmt.Println("The token is")
	fmt.Println(token)

	// Sign and get the complete encoded token as a string
	tokenString, err := token.SignedString([]byte(secretKey))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
		return
	}

	// Return a personalized greeting after successful login.
	c.JSON(http.StatusOK, gin.H{"message": "Hello " + user.Name, "token": tokenString})
}

func authMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		tokenString := c.GetHeader("Authorization")
		if tokenString == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "No token provided"})
			c.Abort()
			return
		}
		// Parse the token
		token, err := jwt.Parse(tokenString, func(token *jwt.Token) (interface{}, error) {
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, http.ErrAbortHandler
			}
			return []byte(secretKey), nil
		})

		if err != nil || !token.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized Nil"})
			c.Abort() // Stop further processing if unauthorized
			return
		}

		// Set the token claims to the context
		if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
			c.Set("claims", claims)
		} else {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized Claim"})
			c.Abort()
			return
		}
		c.Next() // Proceed to the next handler if authorized
	}
}

// landingHandler is a protected route that only logged-in users can access.
func landingHandler(c *gin.Context) {
	// Retrieve claims set by the auth middleware.
	claims, exists := c.Get("claims")
	if !exists {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
		return
	}
	tokenClaims := claims.(jwt.MapClaims)
	// Use the "name" claim to greet the user.
	name, ok := tokenClaims["name"].(string)
	if !ok {
		name = "User"
	}
	c.JSON(http.StatusOK, gin.H{"message": "Hello " + name})
}

func uploadHandler(c *gin.Context) {
	// 1) Parse multipart form
	file, header, err := c.Request.FormFile("video")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "No video file provided"})
		return
	}
	defer file.Close()

	// 2) Determine content-type
	ext := filepath.Ext(header.Filename)
	contentType := mime.TypeByExtension(ext)
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// 3) Use the AWS SDK uploader for efficient multipart uploads
	uploader := manager.NewUploader(s3Client)
	upParams := &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(header.Filename),
		Body:        file,
		ContentType: aws.String(contentType),
	}

	result, err := uploader.Upload(context.TODO(), upParams)
	if err != nil {
		log.Printf("Failed to upload to S3: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Upload to S3 failed"})
		return
	}

	// 4) (Optional) save metadata to MongoDB, e.g.  uploader, timestamp

	c.JSON(http.StatusOK, gin.H{
		"message":  "File uploaded successfully",
		"location": result.Location, // S3 returns the URL in Location

	})
}

// makeDownloadProxy returns a Gin handler that will fetch `remoteURL`
// and serve it to the client as a download named `localFilename`.
func makeDownloadProxy(remoteURL, localFilename string) gin.HandlerFunc {
	return func(c *gin.Context) {
		// 1) Fetch from the remote URL
		fmt.Println("Successfully entered function")
		resp, err := http.Get(remoteURL)
		if err != nil {
			c.String(http.StatusBadGateway, "Eroare fetch remote: %v", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			c.String(resp.StatusCode,
				"Remote a răspuns cu status %d", resp.StatusCode)
			return
		}

		// 2) Propagate Content-Type (optional; defaults to binary)
		contentType := resp.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/octet-stream"
		}
		c.Header("Content-Type", contentType)

		// 3) Force download dialog with the desired filename
		c.Header("Content-Disposition",
			fmt.Sprintf("attachment; filename=\"%s\"", localFilename))

		// 4) Stream the body directly to the client
		c.Status(http.StatusOK)
		_, copyErr := io.Copy(c.Writer, resp.Body)
		if copyErr != nil {
			// The headers are already sent; just log
			fmt.Printf("Eroare la streaming: %v\n", copyErr)
		}
	}
}

// listIntersections handler
func listIntersections(c *gin.Context) {
	// 1) get user folder name from JWT claims
	claimsI, _ := c.Get("claims")
	claims := claimsI.(jwt.MapClaims)
	user := claims["name"].(string) // or "email"

	// 2) list "folders" under user/
	out, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket:    aws.String(bucketName),
		Prefix:    aws.String(user + "/"),
		Delimiter: aws.String("/"),
	})
	if err != nil {
		c.JSON(500, gin.H{"errorlistIntersection": err.Error()})
		return
	}

	// 3) collect the CommonPrefixes (each is user/IntersectionX/)
	var intersections []string
	for _, cp := range out.CommonPrefixes {
		// trim trailing slash and user prefix:
		folder := strings.TrimSuffix(*cp.Prefix, "/")
		parts := strings.Split(folder, "/")
		intersections = append(intersections, parts[len(parts)-1])
	}
	c.JSON(200, intersections)
}

// listImages handler
// func listImages(c *gin.Context) {
// 	ix := c.Param("ix")
// 	claimsI, _ := c.Get("claims")
// 	claims := claimsI.(jwt.MapClaims)
// 	user := claims["name"].(string)

// 	prefix := fmt.Sprintf("%s/%s/", user, ix)
// 	out, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
// 		Bucket: aws.String(bucketName),
// 		Prefix: aws.String(prefix),
// 	})
// 	if err != nil {
// 		c.JSON(500, gin.H{"errorlistImages": err.Error()})
// 		return
// 	}

// 	// Build public URLs (assuming ACL public-read)
// 	base := fmt.Sprintf("https://%s.s3.%s.amazonaws.com/", bucketName, region)
// 	urls := []string{}
// 	for _, obj := range out.Contents {
// 		if strings.HasSuffix(*obj.Key, ".png") {
// 			urls = append(urls, base+*obj.Key)
// 		}
// 	}
// 	c.JSON(200, urls)
// }

func listImages(c *gin.Context) {
	ix := c.Param("ix")
	claimsI, _ := c.Get("claims")
	user := claimsI.(jwt.MapClaims)["name"].(string)

	prefix := fmt.Sprintf("%s/%s/", user, ix)
	out, err := s3Client.ListObjectsV2(context.TODO(), &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}

	presigner := s3.NewPresignClient(s3Client)
	var urls []string
	for _, obj := range out.Contents {
		key := *obj.Key
		if !strings.HasSuffix(key, ".png") {
			continue
		}
		// Generate a presigned GET valid for 15 minutes
		presignedReq, err := presigner.PresignGetObject(context.TODO(), &s3.GetObjectInput{
			Bucket: aws.String(bucketName),
			Key:    aws.String(key),
		}, s3.WithPresignExpires(15*time.Minute))
		if err != nil {
			c.JSON(500, gin.H{"error": "Could not presign URL: " + err.Error()})
			return
		}
		urls = append(urls, presignedReq.URL)
	}

	c.JSON(200, urls)
}

func main() {

	// mongoClient = connectMongo()
	// Use a database named "myapp" and a collection named "users"
	// userCollection = mongoClient.Database("go_app").Collection("user")

	initS3()

	r := gin.Default()

	// CORS configuration using the default settings
	r.Use(cors.New(cors.Config{
		AllowOrigins:     []string{"https://peaceful-dragon-66b0be.netlify.app"},
		AllowMethods:     []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}))

	// Public routes.
	r.POST("/register", register)
	r.POST("/login", login)
	r.GET("/api", makeDownloadProxy(
		"https://elf-wanted-mullet.ngrok-free.app/files/aaa.txt",
		"aaa.txt",
	))

	// Protected routes: only accessible with a valid token.
	authorized := r.Group("/")
	authorized.Use(authMiddleware())
	authorized.GET("/landing", landingHandler)
	// List intersection folders for the current user
	authorized.GET("/intersections", listIntersections)

	// List images in one intersection
	authorized.GET("/intersections/:ix/images", listImages)
	r.POST("/upload", uploadHandler)

	r.Run(":8080")

}
