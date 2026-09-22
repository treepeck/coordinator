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

	log.Print("Parsing COOKIE_KEY environment variable...")
	cookieKey, err := auth.ParseCookieKey(os.Getenv("COOKIE_KEY"))
	if err != nil {
		log.Panic(err)
	}
	log.Print("Successfully parsed COOKIE_KEY.")

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

	ts, err := transport.InitService()
	if err != nil {
		log.Panic(err)
	}

	wsService := ws.InitService(ts.Open, ts.Close, ts.Poke, ts.Write, ts.Read)
	log.Print("Successfully initialized services.")

	log.Print("Connecting to JustChess...")
	req := make(chan error)
	ts.Open <- req
	if err := <-req; err != nil {
		log.Panic(err)
	}
	log.Print("Successfully connected to JustChess.")

	// Register routes.
	log.Print("Registering WebSocket handshake endpoint...")
	mux := http.NewServeMux()
	wsService.RegisterRoutes(authService, mux)
	log.Print("Successfully registered endpoint.")

	log.Print("Starting server.")
	log.Panic(http.ListenAndServe(":8888", mux))
}
