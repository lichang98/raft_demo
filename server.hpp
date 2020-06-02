#pragma once
#include "rpc.hpp"
#include "server_sm.hpp"
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
            this->curr_tm = other.curr_tm;
            this->rpcmanager = other.rpcmanager;

            this->server_identity=other.server_identity;
            this->server_idx=other.server_idx;
            this->leader_id=other.leader_id;
            this->logmanager=other.logmanager;
            this->current_term=other.current_term;
            this->voted_for=other.voted_for;

            this->commit_index=other.commit_index;
            this->last_applied=other.last_applied;
            this->atomic_on_leader_index.clear();

            this->match_index=other.match_index;
            this->leader_commit=other.leader_commit;
            this->is_server_running=other.is_server_running;
            this->get_vote_count=other.get_vote_count;
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
        ServerEnt(int32_t _idx, char* _ip_addr, int32_t _port,\
                const char* opaque_server, const int32_t opaque_server_port)
        {
            server_idx=_idx;
            server_identity = identity::FOLLOWER;
            leader_id=-1;

            is_server_running=true;
            voted_for=-1;
            get_vote_count=0;
            this->current_term=0;
            this->commit_index=-1;
            this->last_applied=-1;
            this->leader_commit=-1;
            this->replicate_count=0;

            rpcmanager.create_socket();
            rpcmanager.bind_socket_port(_port);
            rpc::rpc_data* init_data = new rpc::rpc_data();
            init_data->src_server_index = _idx;
            init_data->type=rpc::rpc_type::CONN;
            LOG(INFO)<< "server " << _idx << ", try connect opaque router";
            rpcmanager.create_connection((char*)opaque_server,opaque_server_port);
            LOG(INFO) << "server " << _idx << ", connected to router, try send conn rpc";
            char* send_ch=nullptr;
            init_data->serialize(send_ch);
            rpcmanager.client_send_msg((void*)send_ch,rpc::rpc_data::serialize_size());
            LOG(INFO) << "server " << _idx << " sent conn to router finish";
            reset_timer();
        }

        void start(){std::thread(&ServerEnt::server_run,this).detach();}

        /*
        * the normal procedures of a server
        * including invoke election when timeout, receive RPCs to append entries .etc
        */
        void server_run()
        {
            while(is_server_running)
            {
                while(atomic_on_leader_index.test_and_set()){std::this_thread::sleep_for(std::chrono::milliseconds(1));}
                if(this->server_identity == identity::LEADER && get_current_tm() >= elect_timeout_high/2)
                {
                    // send AppendEntries RPcs to all other servers
                    for(const std::pair<int32_t, int32_t>& ele : this->match_index)
                    {
                        rpc::rpc_data* sent_data=new rpc::rpc_data();
                        sent_data->src_server_index=this->server_idx;
                        sent_data->dest_server_index=ele.first;
                        sent_data->type=rpc::rpc_type::APPEND_ENTRIES;
                        rpc::rpc_append_entries* param=new rpc::rpc_append_entries();
                        param->term=this->current_term;
                        param->leader_id=this->server_idx;
                        param->leader_commit=this->commit_index;
                        // replicate log entries range from commit index to match index
                        param->prev_log_index=ele.second;
                        param->prev_log_term = logmanager.get_entry_by_index(ele.second).term;
                        std::vector<my_data_type::log_entry> rep_entries;
                        if(ele.second+1 < logmanager.get_log_size())
                            rep_entries = logmanager.get_range(ele.second+1, logmanager.get_log_size()-1);
                        for(int32_t i=0;i<rep_entries.size();++i)
                            param->entries[i] = rep_entries[i];
                        param->real_bring=rep_entries.size();
                        this->commit_index = logmanager.get_log_size()-1;
                        sent_data->params.apd = *param; // append entries params
                        sent_data->is_request=true;
                        LOG(INFO) << "leader " << this->server_idx << ", send AppendEntries RPC to server " << ele.first\
                            << ", log range low=" << ele.second+1 << ", range high=" << logmanager.get_log_size()-1\
                            << ", commit index=" << this->leader_commit << ", current term=" << this->current_term
                            << ", send append entries rpc to " << sent_data->dest_server_index << ", real bring=" << sent_data->params.apd.real_bring;
                        char* send_ch = nullptr;
                        sent_data->serialize(send_ch);
                        rpcmanager.client_send_msg((void*)send_ch, rpc::rpc_data::serialize_size());
                    }
                    atomic_on_leader_index.clear();
                    reset_timer();
                }
                else
                {
                    atomic_on_leader_index.clear();
                    // check if election timeout
                    // random election timeout can avoid conflict, and selected in range 150 to 300 ms
                    int32_t rand_timeout = random()%(elect_timeout_high-elect_timeout_low)\
                                                    +elect_timeout_low;
                    if(this->server_identity == identity::FOLLOWER && get_current_tm() >= rand_timeout)
                    {
                        // invoke election, send RequestVote RPCs to all other servers
                        // during election, past RPCs from other machines may arrive
                        // start a new thread to solve the request vote related processing
                        this->current_term++; // firstly add current term
                        this->server_identity = identity::CANDIDATE;
                        this->voted_for=this->server_idx; // candidate vote for self
                        this->get_vote_count=1;
                        this->leader_id=-1;
                        // send RequestVote RPCs to all other servers
                        rpc::rpc_data* sent_data = new rpc::rpc_data();
                        rpc::rpc_requestvote* req_vote = new rpc::rpc_requestvote();

                        sent_data->src_server_index=this->server_idx;
                        sent_data->dest_server_index=-1;
                        sent_data->type=rpc::rpc_type::REQUEST_VOTE;
                        sent_data->is_request=true;

                        req_vote->candidate_id=this->server_idx;
                        req_vote->term=this->current_term;
                        logmanager.get_last_log_index_term(req_vote->last_log_index,req_vote->last_log_term);
                        sent_data->params.req_vote = *req_vote;
                        LOG(INFO) << "invoke election, current term=" << this->current_term << ", server idx="\
                            << this->server_idx;
                        char* send_ch=nullptr;
                        sent_data->serialize(send_ch);
                        rpcmanager.client_send_msg((void*)send_ch, rpc::rpc_data::serialize_size());
                        reset_timer();
                    }
                    else if(this->server_identity == identity::FOLLOWER)
                    {
                        // followers normal process
                        // check entries to commit
                        this->commit_index = this->leader_commit;
                        LOG(INFO) << "follower server " << this->server_idx << ", normal self process, commit index=" << this->commit_index;
                        if(this->commit_index > this->last_applied)
                        {
                            // apply these entries onto state machine
                            std::vector<my_data_type::log_entry> entry_comit=\
                                            logmanager.get_range(this->last_applied+1,this->commit_index);
                            this->state_machine.update_db(entry_comit);
                            LOG(INFO) << "server idx=" << this->server_idx << ", commit entries from " << this->last_applied+1\
                                << " to " << this->commit_index << ", server statemachine is: " << state_machine;
                            this->last_applied = this->commit_index;
                        }
                    }
                    else if(this->server_identity == identity::CANDIDATE && get_current_tm() >= rand_timeout)
                    {
                        // multi candidate exists, and no leader elected, go back to follower, 
                        // new election start at next term
                        this->server_identity = identity::FOLLOWER;
                        this->voted_for=-1;
                        this->get_vote_count=0;
                        this->leader_id=-1;
                        reset_timer();
                    }
                }
                // try to receive datas
                char* recv_ch = std::get<1>(rpcmanager.recv_data(rpc::rpc_data::serialize_size()));
                rpc::rpc_data* recv_data=nullptr;
                if(recv_ch)
                {
                    recv_data = new rpc::rpc_data();
                    recv_data->deserialize(recv_ch);
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                if(!recv_data)
                    continue;
                if(recv_data->is_request)
                {
                    // this part solve the request from other servers
                    // the next part 'else' solve the response of self's requests
                    // process according to RPC type
                    if(recv_data->type == rpc::rpc_type::CLIENT_REQUEST)
                    {
                        // if current server is leader, append the command into log entries
                        // else the client request need to redirect to current leader
                        if(this->server_identity == identity::LEADER)
                        {
                            // append log entries, and issue AppendEntries RPCs to replicate new entries
                            // rpc::client_request* req = (rpc::client_request*)recv_data->params;
                            rpc::client_request* req = &recv_data->params.clt;
                            std::vector<my_data_type::log_entry> entries(req->data,req->data+(req->real_bring));
                            logmanager.append_entries(entries);
                            int32_t last_term;
                            logmanager.get_last_log_index_term(this->commit_index,last_term);
                            this->leader_commit=this->commit_index;
                            // AppendEntries RPCs will not invoke here, all request send of the leader is
                            // at the beginning of the main loop
                            LOG(INFO) << "server idx=" << this->server_idx << " receive user client request, current is leader"
                                        <<", commit index=" << this->commit_index;
                        }
                        else
                        {
                            // redirect the request to leader
                            recv_data->dest_server_index = this->leader_id;
                            recv_data->is_request=true;
                            if(this->leader_id == -1)
                            {
                                LOG(INFO) << "server idx=" << this->server_idx << ", receive user client request,no leader known to current server, send to self";
                                recv_data->dest_server_index=this->server_idx;
                            }
                            else
                            {
                                LOG(INFO) << "server idx=" << this->server_idx << ", receive user client request, not leader, redirect to leader " << this->leader_id; 
                            }
                            char* sent_ch=nullptr;
                            recv_data->serialize(sent_ch);
                            rpcmanager.client_send_msg((void*)sent_ch, rpc::rpc_data::serialize_size());  
                        }
                    }
                    else if(recv_data->type == rpc::rpc_type::APPEND_ENTRIES)
                    {
                        rpc::rpc_append_entries* recv_params = &recv_data->params.apd;
                        if(this->server_identity == identity::CANDIDATE || this->server_identity == identity::LEADER)
                        {
                            // check the term along with the RPCs, if the term is no smaller than
                            // current server's term, current server change to follower
                            if(recv_params->term >= this->current_term)
                            {
                                LOG(INFO) << "current server is candidate or leader, idx=" << this->server_idx\
                                    << ", confirm src server " << recv_data->src_server_index << " to be leader";
                                this->current_term=recv_params->term;
                                this->server_identity = identity::FOLLOWER;
                                this->leader_id=recv_data->src_server_index;
                                this->get_vote_count=0;
                                this->leader_commit=recv_params->leader_commit;
                                // if param has entries, append entries to target place
                                if(recv_params->real_bring >0)
                                {
                                    int32_t apd_pos=-1;
                                    if((apd_pos=this->logmanager.found_by_term_index(recv_params->prev_log_index,recv_params->prev_log_term)) >=0)
                                    {
                                        // success apply new entries
                                        std::vector<my_data_type::log_entry> apd_entries(recv_params->entries,recv_params->entries+recv_params->real_bring);
                                        logmanager.append_entries_fix_pos(apd_pos,apd_entries);
                                        recv_params->prev_log_index = apd_pos+recv_params->real_bring-1;
                                        recv_params->succ=true;
                                        recv_params->current_term=this->current_term;
                                        LOG(INFO) << "current server idx=" << this->server_idx << ", append entries "<<\
                                                " from pos " << apd_pos << ", length=" << recv_params->real_bring;
                                    }
                                    else
                                    {
                                        LOG(INFO) << "current server idx=" << this->server_idx << ", not found match, apped entry fail" 
                                                << ", src server idx=" << recv_data->src_server_index;
                                        recv_params->succ=false;
                                    }
                                }
                                else
                                {
                                    LOG(INFO) << "current server idx=" << this->server_idx << " entries come with RPC is empty"
                                                << ", src server idx=" << recv_data->src_server_index;
                                    recv_params->succ=true;
                                }
                                reset_timer();
                                voted_for=-1;
                            }
                            else
                            {
                                LOG(INFO) << "current server idx=" << this->server_idx << ", comes term is lower, rejected";
                                recv_params->succ=false;
                                recv_params->current_term=this->current_term; // send back latest term
                            }
                        }
                        else
                        {
                            // check term along with the RPC
                            rpc::rpc_append_entries* recv_params = &recv_data->params.apd;
                            this->leader_commit = recv_params->leader_commit;
                            if(recv_params->term < this->current_term)
                            {
                                recv_params->succ=false;
                                recv_params->current_term=this->current_term;
                                LOG(INFO) << "current server " << this->server_idx << " is a follower, reject invalid appendEntries RPC";
                            }
                            else
                            {
                                // if param has entries, append entries to target place
                                if(recv_params->real_bring >0)
                                {
                                    int32_t apd_pos=-1;
                                    this->current_term=recv_params->term;
                                    if((apd_pos=this->logmanager.found_by_term_index(recv_params->prev_log_index,recv_params->prev_log_term)) >=0)
                                    {
                                        std::vector<my_data_type::log_entry> apd_entries(recv_params->entries,recv_params->entries+recv_params->real_bring);
                                        logmanager.append_entries_fix_pos(apd_pos,apd_entries);
                                        recv_params->prev_log_index = apd_pos-1+recv_params->real_bring;
                                        recv_params->succ=true;
                                        LOG(INFO) << "current server idx=" << this->server_idx << ", append entries "<<\
                                                " from pos " << apd_pos << ", length=" << recv_params->real_bring << " src server=" << recv_data->src_server_index;
                                    }
                                    else
                                    {
                                        LOG(INFO) << "current server idx=" << this->server_idx << ", reject because no match found "
                                            << ", prev log index=" << recv_params->prev_log_index;
                                        recv_params->succ=false;
                                    }
                                }
                                else
                                {
                                    LOG(INFO) << "current server idx=" << this->server_idx << ", comes entries is empty, RPC from " << recv_data->src_server_index
                                                << ", current servr commit index=" << this->commit_index;
                                    recv_params->succ=true;
                                    recv_params->prev_log_index=this->commit_index;
                                }
                                this->leader_id=recv_data->src_server_index;
                                reset_timer();
                            }
                        }
                        recv_data->is_request=false;
                        std::swap(recv_data->src_server_index,recv_data->dest_server_index);
                        char* sent_ch=nullptr;
                        recv_data->serialize(sent_ch);
                        rpcmanager.client_send_msg((void*)sent_ch, rpc::rpc_data::serialize_size());
                    }
                    else if(recv_data->type == rpc::rpc_type::REQUEST_VOTE)
                    {
                        // Reply false if term < currentTerm
                        // If votedFor is null or candidateId, and candidate’s log is at
                        // least as up-to-date as receiver’s log, grant vote
                        rpc::rpc_requestvote* param = &recv_data->params.req_vote;
                        if(param->term <= this->current_term)
                        {
                            param->current_term=this->current_term;
                            param->vote_granted=false;
                            LOG(INFO) << "current server idx=" << this->server_idx<< ", reject invalid RequestVote";
                        }
                        else
                        {
                            int32_t last_index=0,last_term=0;
                            logmanager.get_last_log_index_term(last_index,last_term);
                            if((this->voted_for == -1 || this->voted_for==this->server_idx) &&\
                                    param->last_log_index >= last_index && param->last_log_term >= last_term)
                            {
                                param->vote_granted=true;
                                this->voted_for=param->candidate_id;
                                this->server_identity = identity::FOLLOWER;
                                this->get_vote_count=0;
                                LOG(INFO) << "current server idx=" << this->server_idx << ", grant vote to " << param->candidate_id;
                            }
                            else
                            {
                                param->vote_granted=false;
                                LOG(INFO) << "current server idx=" << this->server_idx << ", no grant vote to " << param->candidate_id;
                            }
                            param->current_term=this->current_term;
                            this->current_term=param->term;
                            reset_timer();
                        }
                        recv_data->is_request=false;
                        std::swap(recv_data->src_server_index,recv_data->dest_server_index);
                        char* sent_ch=nullptr;
                        recv_data->serialize(sent_ch);
                        rpcmanager.client_send_msg((void*)sent_ch, rpc::rpc_data::serialize_size());
                    }
                    else if(recv_data->type == rpc::rpc_type::INSTALL_SNAPSHOT)
                    {
                        recv_data->is_request=false;
                        std::swap(recv_data->src_server_index,recv_data->dest_server_index);
                        char* sent_ch=nullptr;
                        recv_data->serialize(sent_ch);
                        rpcmanager.client_send_msg((void*)sent_ch, rpc::rpc_data::serialize_size());
                    }
                }
                else
                {
                    // read data is response to self's requests
                    // if current server is a leader, the possible RPCs responses is AppendEntries
                    // if a candidate, the possible RPCs responses is RequestVote
                    // followers will not send RPCs proactively, thus no response
                    if(this->server_identity ==identity::LEADER)
                    {
                        rpc::rpc_append_entries* param = &recv_data->params.apd;
                        if(recv_data->type == rpc::rpc_type::APPEND_ENTRIES)
                        {
                            if(!param->succ)
                            {
                                // append entry RPC is not success, since not found in follower's entry
                                // decrement and retry
                                // the retry sent is not processed here, this part only update records according responses
                                match_index[recv_data->src_server_index]=param->prev_log_index-1;
                                LOG(INFO) << "current server idx=" << this->server_idx << ", Append Entries RPC response fail," <<\
                                            " log index minus 1, =" << param->prev_log_index-1 << ", for dest follower " << recv_data->src_server_index;
                            }
                            else
                            {
                                match_index[recv_data->src_server_index]=param->prev_log_index;
                                LOG(INFO) << "leader " << this->server_idx << ", update server " << recv_data->src_server_index
                                    << "'s match index =" << param->prev_log_index;
                                // the response received may  for previous request, it can be checked by leader commit
                                // only response for current requests counted
                                if(recv_data->params.apd.leader_commit == this->leader_commit)
                                {
                                    replicate_count++;
                                    LOG(INFO) << "current server idx=" << this->server_idx << ", Append Entries RPC response success from " << recv_data->src_server_index
                                        << ", currently total replicate count=" << replicate_count;
                                    if(commit_index > last_applied && replicate_count >= num_majority)
                                    {
                                        // log entries have been replicated on major followers
                                        // the entries before can be committed safely
                                        LOG(INFO) << "current server idx=" << this->server_idx << ", has replicated on major followers"
                                                << ", commit index=" << commit_index << ", last_applied=" << last_applied;
                                        std::vector<my_data_type::log_entry> comit_logs = logmanager.get_range(last_applied+1, commit_index);
                                        state_machine.update_db(comit_logs);
                                        last_applied=commit_index;
                                        this->leader_commit=commit_index;
                                        replicate_count=0;
                                        LOG(INFO) << "current server idx=" << this->server_idx << ", update statemachine,"
                                                << " state is :" << this->state_machine;
                                    }
                                    else if(commit_index <= last_applied)
                                    {
                                        replicate_count=0;
                                    }
                                }
                            }
                        }
                    }
                    else if(this->server_identity == identity::CANDIDATE)
                    {
                        // current server is a candidate, possible responses of RPCs is RequestVote
                        if(recv_data->type == rpc::rpc_type::REQUEST_VOTE)
                        {
                            rpc::rpc_requestvote* rpc_param = &recv_data->params.req_vote;
                            LOG(INFO) << "current server idx=" << this->server_idx << ", get response from "\
                                <<  recv_data->src_server_index << ", flag of grant vote=" << rpc_param->vote_granted;
                            if(rpc_param->vote_granted)
                                get_vote_count++;
                            if(get_vote_count >= num_majority)
                            {
                                // this server get the majority of the votes, become leader
                                this->server_identity = identity::LEADER;
                                this->leader_id=this->server_idx;
                                get_vote_count=0;
                                voted_for=-1;
                                LOG(INFO) << "current server idx=" << this->server_idx << ", get major votes become leader";
                            }
                        }
                    }
                }
            }
            // release resources
            rpcmanager.close_socket();
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
        server_sm::SM state_machine; // each server's state_machine, a K-V in memory database
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
        int32_t get_vote_count;
        int32_t replicate_count;
        static const int32_t elect_timeout_low=450;
        static const int32_t elect_timeout_high=600;
        static int32_t num_majority;
        static int32_t num_servers;
    };
    int32_t ServerEnt::num_majority;
    int32_t ServerEnt::num_servers;
    
} // namespace server
