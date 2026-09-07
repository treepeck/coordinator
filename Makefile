include ../../config/common.env
export

normal:
	go run -race cmd/coordinator/main.go

dlv:
	dlv --headless --listen localhost:50000 debug cmd/coordinator/main.go

gdlv:
	gdlv connect localhost:50000