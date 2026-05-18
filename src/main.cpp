#include <iostream>
#include <memory>
#include <string>

#include <grpcpp/grpcpp.h>

#include "hello.grpc.pb.h"


class GreeterService final : public demo::Greeter::Service {
public:
    grpc::Status SayHello(
        grpc::ServerContext* context,
        const demo::HelloRequest* request,
        demo::HelloReply* reply
    ) override {
        reply->set_message("Hello, " + request->name());
        return grpc::Status::OK;
    }
};

int main() {
    const std::string address = "0.0.0.0:50051";

    GreeterService service;

    grpc::ServerBuilder builder;
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.RegisterService(&service);

    std::unique_ptr<grpc::Server> server = builder.BuildAndStart();

    std::cout << "Server listening on " << address << '\n';

    server->Wait();

    return 0;
}
