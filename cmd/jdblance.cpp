// jdblance: the load balancer.  See src/server.hpp for the process model and
// src/proxy.hpp for the datapath.
#include "server.hpp"

int main(int argc, char** argv) { return jdblance::main_cli(argc, argv); }
