#include "rpc.hpp"
#include "log_manager.hpp"
#include <chrono>
#include <random>
#include <thread>
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
        ServerEnt(){}

        ServerEnt(const ServerEnt& other)
        {
            this->begin_tm = other.begin_tm;
            this->curr_tm=other.curr_tm;
            this->rpcmanager = other.rpcmanager;

            this->server_identity = other.server_identity;
            this->server_idx=other.server_idx;
            this->leader_id=other.leader_id;
            this->logmanager=other.logmanager;

            this->current_term=other.current_term;
            this->voted_for=other.voted_for;

            this->commit_index=other.commit_index;
            this->last_applied=other.last_applied;

            this->match_index=other.match_index;
            this->leader_commit=other.leader_commit;

            this->is_server_running=other.is_server_running;
            this->on_election=other.on_election;   

            this->atomic_on_leader_index.clear(); 
        }
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
                const char* opaque_server, const int32_t opaque_server_port)
        {
            server_idx=_idx;
            server_identity = _iden;
            leader_id=-1;

            is_server_running=true;
            on_election=false;
            voted_for=-1;
            get_vote_count=0;
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
                    // send empty  AppendEntries RPcs to all other servers as the heartbeat
                    rpc::rpc_data* sent_data = new rpc::rpc_data();
                    sent_data->src_server_index=this->server_idx;
                    sent_data->dest_server_index=-1; // to all
                    sent_data->type = rpc::rpc_type::APPEND_ENTRIES;
                    rpc::rpc_append_entries* param = new rpc::rpc_append_entries();
                    param->term = this->current_term;
                    param->leader_id = this->server_idx;
                    logmanager.get_last_log_index_term(param->prev_log_index,param->prev_log_term);
                    param->entries=nullptr;
                    param->leader_commit = this->commit_index;
                    sent_data->params = param;
                    sent_data->is_request=true;

                    rpcmanager.client_send_msg((void*)sent_data, sizeof(rpc::rpc_data));
                    atomic_on_leader_index.clear();
                    reset_timer();
                    on_election=false;
                }
                else
                {
                    atomic_on_leader_index.clear();
                    // check if election timeout
                    // random election timeout can avoid conflict, and selected in range 150 to 300 ms
                    int32_t rand_timeout = random()%(elect_timeout_high-elect_timeout_low)\
                                                    +elect_timeout_low;
                    if(!on_election && get_current_tm() >= rand_timeout)
                    {
                        // invoke election, send RequestVote RPCs to all other servers
                        // during election, past RPCs from other machines may arrive
                        // start a new thread to solve the request vote related processing
                        this->current_term++; // firstly add current term
                        this->server_identity = identity::CANDIDATE;
                        // send RequestVote RPCs to all other servers
                        on_election = true;
                        rpc::rpc_data* sent_data = new rpc::rpc_data();
                        rpc::rpc_requestvote* req_vote = new rpc::rpc_requestvote();

                        sent_data->src_server_index=this->server_idx;
                        sent_data->dest_server_index=-1;
                        sent_data->type=rpc::rpc_type::REQUEST_VOTE;
                        sent_data->is_request=true;

                        req_vote->candidate_id=this->server_idx;
                        req_vote->term=this->current_term;
                        logmanager.get_last_log_index_term(req_vote->last_log_index,req_vote->last_log_term);
                        sent_data->params = req_vote;
                        rpcmanager.client_send_msg((void*)sent_data,sizeof(rpc::rpc_data));
                        reset_timer();
                    }
                }
                // try to receive datas
                rpc::rpc_data* recv_data = (rpc::rpc_data*)std::get<1>(rpcmanager.recv_data());
                if(!recv_data)
                    continue;
                if(recv_data->is_request)
                {
                    // process according to RPC type
                    if(recv_data->type == rpc::rpc_type::APPEND_ENTRIES)
                    {
                        rpc::rpc_append_entries* recv_params = (rpc::rpc_append_entries*)recv_data->params;
                        if(this->server_identity == identity::CANDIDATE|| this->server_identity == identity::LEADER)
                        {
                            // check the term along with the RPCs, if the term is no smaller than
                            // current server's term, current server change to follower
                            if(recv_params->term >= this->current_term)
                            {
                                this->current_term=recv_params->term;
                                this->server_identity = identity::FOLLOWER;
                                // if param has entries, append entries to target place
                                int32_t apd_pos=-1;
                                if((apd_pos=this->logmanager.found_by_term_index(recv_params->prev_log_index,recv_params->prev_log_term)) >=0)
                                {
                                    // success apply new entries
                                    logmanager.append_entries_fix_pos(apd_pos,*((std::vector<log_manager::log_entry>*)recv_params->entries));
                                    recv_params->succ=true;
                                    recv_params->current_term=this->current_term;
                                    on_election=false;
                                }
                                else
                                {
                                    recv_params->succ=false;
                                }
                            }
                            else
                            {
                                recv_params->succ=false;
                                recv_params->current_term=this->current_term;
                            }
                        }
                        else
                        {
                            // check term along with the RPC
                            rpc::rpc_append_entries* recv_params = (rpc::rpc_append_entries*)recv_data->params;
                            if(recv_params->term < this->current_term)
                            {
                                recv_params->succ=false;
                                recv_params->current_term=this->current_term;
                            }
                            else
                            {
                                // if param has entries, append entries to target place
                                int32_t apd_pos=-1;
                                this->current_term = recv_params->term;
                                if((apd_pos=this->logmanager.found_by_term_index(recv_params->prev_log_index,recv_params->prev_log_term)) >=0)
                                {
                                    // success apply new entries
                                    logmanager.append_entries_fix_pos(apd_pos,*((std::vector<log_manager::log_entry>*)recv_params->entries));
                                    recv_params->succ=true;
                                    recv_params->current_term=this->current_term;
                                }
                                else
                                {
                                    recv_params->succ=false;
                                }
                            }
                        }
                        rpcmanager.client_send_msg((void*)recv_data,sizeof(rpc::rpc_data));
                    }
                    else if(recv_data->type == rpc::rpc_type::REQUEST_VOTE)
                    {
                        // Reply false if term < currentTerm
                        // If votedFor is null or candidateId, and candidate’s log is at
                        // least as up-to-date as receiver’s log, grant vote
                        rpc::rpc_requestvote* param = (rpc::rpc_requestvote*)recv_data->params;
                        if(param->term < this->current_term)
                        {
                            param->term=this->current_term;
                            param->vote_granted=false;
                        }
                        else
                        {
                            int32_t last_index=0,last_term=0;
                            logmanager.get_last_log_index_term(last_index,last_term);
                            if((this->voted_for == -1 || this->voted_for==param->candidate_id) &&\
                                    param->last_log_index >= last_index && param->last_log_term >= last_term)
                            {
                                param->vote_granted=true;
                                this->voted_for=param->candidate_id;
                            }
                            on_election=true;
                        }
                        rpcmanager.client_send_msg((void*)recv_data,sizeof(rpc::rpc_data));
                    }
                    else if(recv_data->type == rpc::rpc_type::INSTALL_SNAPSHOT)
                    {

                        rpcmanager.client_send_msg((void*)recv_data,sizeof(rpc::rpc_data));
                    }
                }
                else
                {
                    // read data is response
                    // if current server is a leader, the possible RPCs responses is AppendEntries
                    // if a candidate, the possible RPCs responses is RequestVote
                    // followers will not send RPCs proactively, thus no response
                    if(this->server_identity ==identity::LEADER)
                    {
                        rpc::rpc_append_entries* param = (rpc::rpc_append_entries*)recv_data->params;
                        if(recv_data->type == rpc::rpc_type::APPEND_ENTRIES)
                        {
                            if(!param->succ)
                            {
                                // append entry RPC is not success, since not found in follower's entry
                                // decrement and retry
                                param->prev_log_index--;
                                log_manager::log_entry new_entry = logmanager.get_entry_by_index(param->prev_log_index);
                                param->prev_log_term = new_entry.term;
                                std::vector<log_manager::log_entry> *entries = (std::vector<log_manager::log_entry>*)param->entries;
                                entries->insert(entries->begin(),new_entry);
                                recv_data->is_request=true;
                                std::swap(recv_data->src_server_index,recv_data->dest_server_index);
                                rpcmanager.client_send_msg((void*)recv_data,sizeof(rpc::rpc_data));
                            }
                            else
                            {
                                match_index[recv_data->src_server_index]=param->prev_log_index;
                            }
                        }
                    }
                    else if(this->server_identity == identity::CANDIDATE)
                    {
                        // current server is a candidate, possible responses of RPCs is RequestVote
                        rpc::rpc_requestvote* rpc_param = (rpc::rpc_requestvote*)recv_data->params;
                        if(rpc_param->vote_granted)
                            get_vote_count++;
                        if(get_vote_count >= num_majority)
                        {
                            // this server get the majority of the votes, become leader
                            this->server_identity = identity::LEADER;
                        }
                    }
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
        // this atomic variable is used to ensure that leader_index is accessed separately
        std::atomic_flag atomic_on_leader_index=ATOMIC_FLAG_INIT;

        // special for leaders
        // index of highest log entry known to be replicated on server
        std::unordered_map<int32_t, int32_t> match_index; 
        int32_t leader_commit; // leader's commit index

        bool is_server_running;
        bool on_election;
        int32_t get_vote_count;
        static const int32_t elect_timeout_low=150;
        static const int32_t elect_timeout_high=300;
        static int32_t num_majority;
        static int32_t num_servers;
    };
    int32_t ServerEnt::num_majority;
    int32_t ServerEnt::num_servers;
    
} // namespace server
