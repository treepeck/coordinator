Coordinator must be started only after `[JustChess](https://github.com/treepeck/justchess)`
and database are up and running.

The initialization process includes the following steps:
- Set logging flags;
- Parse secret key from environment variable to be able to validate cookies;
- Open a connection with database;
- Initialize database repository;
- Initialize authorization service;
- Initialize transport service;
- Initialize websocket service;
- Open a TCP socket with `[JustChess](https://github.com/treepeck/justchess)`;
- Register HTTP route for websocket handshake;
- Listen and serve HTTP traffic on port 8888.

If at least one of the above steps fails, the Coordinator panics and stops the execution.