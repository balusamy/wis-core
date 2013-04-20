#include "zmqpp/zmqpp.hpp"
#include <boost/format.hpp>
#include <boost/thread.hpp>

//  Provide random number from 0..(num-1)
#if (defined (__WINDOWS__)) || (defined (__UTYPE_IBMAIX)) || (defined (__UTYPE_HPUX)) || (defined (__UTYPE_SUNOS))
#   define randof(num)  (int) ((float) (num) * rand () / (RAND_MAX + 1.0))
#else
#   define randof(num)  (int) ((float) (num) * random () / (RAND_MAX + 1.0))
#endif

static void * client_task ()
{
    zmqpp::context ctx;
    zmqpp::socket client(ctx, zmqpp::socket_type::dealer);

    //  Set random identity to make tracing easier
    std::string identity = str(boost::format("%04X-%04X") 
            % randof (0x10000) % randof (0x10000));

    client.set(zmqpp::socket_option::identity, identity);

    client.connect("tcp://localhost:5570");

    zmqpp::poller poller;
    poller.add(client);
    int request_nbr = 0;
    while (true) {
        //  Tick once per second, pulling in arriving messages
        int centitick;
        for (centitick = 0; centitick < 50; centitick++) {
            poller.poll(10);
            if (poller.events(client) & zmqpp::poller::POLL_IN) {
                zmqpp::message_t msg;
                client.receive(msg);
                std::cout << identity << " - " << msg.get<char*>(msg.parts() - 1) << std::endl;
            }
        }
        client.send(str(boost::format("request #%d") % ++request_nbr));
    }
    return NULL;
}

//  This is our server task.
//  It uses the multithreaded server model to deal requests out to a pool
//  of workers and route replies back to clients. One worker can handle
//  one request at a time but one client can talk to multiple workers at
//  once.

static void server_worker (zmqpp::context *ctx);

void *server_task ()
{
    zmqpp::context ctx;

    //  Frontend socket talks to clients over TCP
    zmqpp::socket frontend(ctx, zmqpp::socket_type::router);
    frontend.bind("tcp://*:5570");

    //  Backend socket talks to workers over inproc
    zmqpp::socket backend(ctx, zmqpp::socket_type::dealer);
    backend.bind("inproc://backend");

    //  Launch pool of worker threads, precise number is not critical
    int thread_nbr;
    for (thread_nbr = 0; thread_nbr < 5; thread_nbr++)
        boost::thread t(boost::bind(server_worker, &ctx));

    //  Connect backend to frontend via a proxy
    zmq_proxy (frontend, backend, NULL);

    return NULL;
}

//  Each worker task works on one request at a time and sends a random number
//  of replies back, with random delays between replies:

static void
server_worker (zmqpp::context *ctx)
{
    zmqpp::socket worker(*ctx, zmqpp::socket_type::dealer);
    worker.connect("inproc://backend");

    while (true) {
        //  The DEALER socket gives us the reply envelope and message
        zmqpp::message_t msg;
        worker.receive(msg);
        std::string identity = msg.get(0);
        std::string content = msg.get(1);
        //assert (content);

        //  Send 0..4 replies back
        int reply, replies = randof (5);
        for (reply = 0; reply < replies; reply++) {
            //  Sleep for some fraction of a second
            boost::this_thread::sleep (boost::posix_time::milliseconds(randof (1000) + 1));
            zmqpp::message out;
            out << identity << content;
            worker.send(out);
        }
    }
}

int main()
{
    boost::thread c1(&client_task);
    boost::thread c2(&client_task);
    boost::thread c3(&client_task);
    boost::thread s(&server_task);

    boost::this_thread::sleep(boost::posix_time::seconds(5));

    return 0;
}
