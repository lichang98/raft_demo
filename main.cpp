#include "system.hpp"

int main(int argc, char const *argv[])
{
    
    my_system::MySys sys;
    sys.parse_json_config("./sys_config.json");

    return 0;
}
