#include "rpc.hpp"
#include "log_manager.hpp"
#include <stdlib.h>
#include <vector>
#include <chrono>
#include <pthread.h>
#include <random>
#include <thread>
#include <unordered_map>
#include <future>
#include <functional>
#include <atomic>

namespace server
{
    enum identity
    {
        FOLLOWER,
        CANDIDATE,
        LEADER
    };
    class ServerEnt
    {
    public:
        /*
        * initial server entity
        * params:
        *   _idx: the global index of the server
        *   _iden: the initial identity of server, all followers
        *   _ip_addr: ip address
        *   _port: socket port
        *   _elec_timeout_low,_elec_timeout_high: for random election timeout
        */
        ServerEnt(int32_t _idx, identity _iden, char* _ip_addr, int32_t _port,\
                int32_t _elec_timeout_low, int32_t _elec_timeout_high, \
                const char* opaque_server, const int32_t opaque_server_port)
        {
            server_idx=_idx;
            server_identity = _iden;
            leader_id=-1;
            election_timeout_low=_elec_timeout_low;
            election_timeout_high=_elec_timeout_high;

            is_server_running=true;
            on_election=false;
            voted_for=-1;
            reset_timer();

            rpcmanager.create_socket();
            rpcmanager.bind_socket_port(_port);
            rpc::rpc_data* init_data = new rpc::rpc_data();
            init_data->src_server_index = _idx;
            init_data->type=rpc::rpc_type::CONN;
            rpcmanager.create_connection((char*)opaque_server,opaque_server_port);
            rpcmanager.client_send_msg((void*)init_data,sizeof(rpc::rpc_data));

            server_run();
        }
        /*
        * the normal procedures of a server
        * including invoke election when timeout, receive RPCs to append entries .etc
        */
        void server_run()
        {
            while(is_server_running)
            {
                while(atomic_on_leader_index.test_and_set()){std::this_thread::sleep_for(std::chrono::milliseconds(1));}
                if(this->server_identity == identity::LEADER)
                {
                    // TODO
                    // send empty  AppendEntries RPcs to all other servers

                    atomic_on_leader_index.clear();
                }
                else
                {
                    atomic_on_leader_index.clear();
                    // check if election timeout
                    // random election timeout can avoid conflict, and selected in range 150 to 300 ms
                    int32_t rand_timeout = random()%(election_timeout_high-election_timeout_low)\
                                                    +election_timeout_low;
                    if(!on_election && get_current_tm() >= rand_timeout)
                    {
                        // invoke election, send RequestVote RPCs to all other servers
                        // during election, past RPCs from other machines may arrive
                        // start a new thread to solve the request vote related processing
                        this->current_term++; // firstly add current term
                        // TODO
                        // send RequestVote RPCs to all other servers
                        on_election = true;
                        reset_timer();
                    }

                }
                // try to receive datas
                rpc::rpc_data* recv_data = (rpc::rpc_data*)std::get<1>(rpcmanager.recv_data());
                if(!recv_data)
                    continue;
                // process according to RPC type
                if(recv_data->type == rpc::rpc_type::APPEND_ENTRIES)
                {
                    if(this->server_identity == identity::CANDIDATE|| this->server_identity == identity::LEADER)
                    {

                    }
                    else
                    {
                        
                    }
                    
                }
                else if(recv_data->type == rpc::rpc_type::REQUEST_VOTE)
                {

                }
                else if(recv_data->type == rpc::rpc_type::INSTALL_SNAPSHOT)
                {

                }
            }
        }

        void reset_timer()
        {
            begin_tm = std::chrono::system_clock::now();
        }

        int64_t get_current_tm()
        {
            curr_tm=std::chrono::system_clock::now();
            std::chrono::milliseconds dura = \
                std::chrono::duration_cast<std::chrono::milliseconds>(curr_tm-begin_tm);
            return dura.count();
        }

        std::chrono::system_clock::time_point begin_tm,curr_tm;
        rpc::RPCManager rpcmanager;
        
        identity server_identity; // the server be follower,candidate or leader
        int32_t server_idx; // the index of the server globally
        int32_t leader_id;
        log_manager::MachineLog logmanager; // the log entries of this server
        int32_t current_term;
        int32_t voted_for; // the candidate that receive vote in current term
        int32_t commit_index; // index of highest log entry known to be committed
        int32_t last_applied; // index of highest log applied to state machine
        // after this time, if not receive any valid RPC, start election
        // and change to candidate
        int32_t election_timeout_low,election_timeout_high;
        // since a candidate can be the leader when gets majority votes,
        // it should not be affected by some very slow machine,
        // server will wait for the responses of requestvote RPCs
        // if not receive response after timeout, it will check others
        const static int32_t vote_back_wait_timeout=10; // milliseconds
        // each server has a passive port listen to the RPCs sent from leader
        // or candidates. thus, each server need to create a thread to asynchronously
        // listen the possible RPCs, using a loop to try to read data from connections
        // built before, there's a timeout, if no data, try next
        const static int32_t passive_read_timeout=10; // milliseconds
        // this atomic variable is used to ensure that leader_index is accessed separately
        std::atomic_flag atomic_on_leader_index = ATOMIC_FLAG_INIT;

        // special for leaders
        std::unordered_map<int32_t, int32_t> next_index; // for each server, record next log entry
        // index of highest log entry known to be replicated on server
        std::vector<int32_t> match_index; 
        int32_t leader_commit; // leader's commit index

        bool is_server_running;
        bool on_election;
    };
    
} // namespace server
