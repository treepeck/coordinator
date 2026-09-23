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
	log.Print("Initializing database repositories...")
	ar := db.NewSQLAuthRepo(pool)
	gr := db.NewSQLGameRepo(pool)
	log.Print("Successfully initialized database repositories.")

	log.Print("Initializing services...")
	authService := auth.NewService(cookieKey, ar)

	transportService, err := transport.InitService()
	if err != nil {
		log.Panic(err)
	}

	wsService := ws.InitService(gr, transportService.Ipc)
	log.Print("Successfully initialized services.")

	log.Print("Connecting to JustChess...")
	req := make(chan int, 1)
	transportService.Ipc.Open <- req
	if <-req != 1 {
		log.Panic("Couldn't connect to JustChess.")
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
