package main

import (
	"log"
	"net/http"
	"os"

	"github.com/treepeck/justchess/pkg/auth"
	"github.com/treepeck/justchess/pkg/db"

	"github.com/treepeck/coordinator/internal/ws"
	"github.com/treepeck/coordinator/internal/transport"
)

func main() {
	log.SetFlags(log.Lshortfile | log.Ldate | log.Ltime)

	cookieKey, err := auth.ParseCookieKey(os.Getenv("COOKIE_KEY"))
	if err != nil {
		log.Panic(err)
	}

	log.Print("Connecting to db...")
	pool, err := db.OpenDB(os.Getenv("DB_DSN"))
	if err != nil {
		log.Panic(err)
	}
	defer pool.Close()
	log.Print("Successfully connected to db.")

	// Initialize database repository.
	ar := db.NewSQLAuthRepo(pool)

	log.Print("Initializing services...")
	authService := auth.NewService(cookieKey, ar)

	wsService := ws.InitService()
	transportService, err := transport.InitService()
	if err != nil {
		log.Panic(err)
	}

	// Register routes.
	mux := http.NewServeMux()
	wsService.RegisterRoutes(authService, mux)

	if err := transportService.OpenSocket(); err != nil {
		log.Panic(err)
	}

	log.Print("Starting server.")
	log.Panic(http.ListenAndServe(":8888", mux))
}
