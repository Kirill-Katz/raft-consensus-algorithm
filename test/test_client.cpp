#include <iostream>
#include <memory>

#include <grpcpp/grpcpp.h>

#include "hello.grpc.pb.h"

int main() {
    auto channel = grpc::CreateChannel(
        "localhost:50051",
        grpc::InsecureChannelCredentials()
    );

    auto stub = demo::Greeter::NewStub(channel);

    demo::HelloRequest request;
    request.set_name("test");

    demo::HelloReply reply;

    grpc::ClientContext context;

    grpc::Status status =
        stub->SayHello(&context, request, &reply);

    if (!status.ok()) {
        std::cerr << status.error_message() << '\n';
        return 1;
    }

    std::cout << reply.message() << '\n';

    return 0;
}
