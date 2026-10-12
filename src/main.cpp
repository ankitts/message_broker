#include "broker/broker.h"
#include "broker/command_handler.h"
#include "broker/tcp_server.h"

int main(){
    // Creation order matters: destruction runs in reverse, so the server goes
    // first, then the handler, then the broker they both depend on.
    Broker broker;
    CommandHandler handler(broker);
    TcpServer server(handler, 8080);
    if (!server.start()) {
        return 1;
    }
    server.run();
    return 0;
}
