#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <unordered_map>
#include <random>
#include <algorithm>
#include <climits>

using namespace std;

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100

#define DOWN_TAG 100 // incoming messages to download thread
#define UP_TAG 101 // incoming messages to upload thread

#define FILE_REQ 1 // send swarm
#define SEED_REQ 2 // change status to seeding
#define FINISH_REQ 3 // client finished downloading
#define UPDATE_REQ 4 // update swarm

pthread_mutex_t upload_mutex;
pthread_mutex_t file_mutex;

struct File {
    string name;
    int num_segments;
    vector<string> segment_hashes;
};

struct PeerData {
    vector<File> owned_files;
    vector<string> desired_files;
};

PeerData peer_data;

PeerData parse_input_file(int rank) {
    PeerData peer_data;
    string file_path = "in" + to_string(rank) + ".txt";
    ifstream input_file(file_path);

    if (!input_file.is_open()) {
        exit(EXIT_FAILURE);
    }

    int num_owned_files;
    input_file >> num_owned_files;
    // send nr of owned files to tracker
    MPI_Send(&num_owned_files, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);

    for (int i = 0; i < num_owned_files; i++) {
        File file;
        input_file >> file.name >> file.num_segments;
        // send file name and nr of segments to tracker
        MPI_Send(file.name.c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        MPI_Send(&file.num_segments, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);

        for (int j = 0; j < file.num_segments; j++) {
            string hash;
            input_file >> hash;
            file.segment_hashes.push_back(hash);

            // send each hash to tracker
            MPI_Send(hash.c_str(), HASH_SIZE, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
        }
        peer_data.owned_files.push_back(file);
    }

    int num_desired_files;
    input_file >> num_desired_files;

    for (int i = 0; i < num_desired_files; i++) {
        string file_name;
        input_file >> file_name;
        peer_data.desired_files.push_back(file_name);
    }

    input_file.close();
    return peer_data;
}

void *download_thread_func(void *arg) {
    int rank = *(int*) arg;
    int downloadedSegments = 0;
    bool downloading = true;
    int downloadedFiles = 0;

    while (downloading) {
        for (string file : peer_data.desired_files) {

            // request file from tracker
            int msg = 1;
            MPI_Send(&msg, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);
            MPI_Send(file.c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);

            // receive swarm
            int swarm_size;
            vector<int> swarm;
            MPI_Recv(&swarm_size, 1, MPI_INT, TRACKER_RANK, DOWN_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            for (int i = 0; i < swarm_size; i++) {
                int peer;
                MPI_Recv(&peer, 1, MPI_INT, TRACKER_RANK, DOWN_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                swarm.push_back(peer);
            }

            // receive hashes
            int num_hashes;
            MPI_Recv(&num_hashes, 1, MPI_INT, TRACKER_RANK, DOWN_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            vector<string> hashes;
            for (int i = 0; i < num_hashes; i++) {
                char hash[HASH_SIZE + 1];
                memset(hash, 0, HASH_SIZE + 1);
                MPI_Recv(hash, HASH_SIZE, MPI_CHAR, TRACKER_RANK, DOWN_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                hashes.push_back(string(hash));
            }

            // download file
            for (int i = 0; i < num_hashes; i++) {
                int check = 0;
                while (true) {
                    int min = INT_MAX;
                    int min_client = -1;

                    for (int client : swarm) {
                        // cant download from yourself
                        if (client != rank) {
                            // request usage count
                            char request[HASH_SIZE + 1] = "USAGE";
                            MPI_Send(request, HASH_SIZE, MPI_CHAR, client, UP_TAG, MPI_COMM_WORLD);

                            int usage_count;
                            MPI_Recv(&usage_count, 1, MPI_INT, client, DOWN_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                            // find client with lowest usage
                            if (usage_count < min) {
                                min = usage_count;
                                min_client = client;
                            }
                        }
                    }

                    MPI_Send(hashes[i].c_str(), HASH_SIZE, MPI_CHAR, min_client, UP_TAG, MPI_COMM_WORLD);

                    // receive confirmation
                    MPI_Recv(&check, 1, MPI_INT, min_client, DOWN_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    if (check == 1) {
                        break;
                    }
                }
                downloadedSegments++;

                // update swarms every 10 segments
                if (downloadedSegments == 10) {
                    int msg_type = 4;
                    MPI_Send(&msg_type, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);
                    MPI_Send(file.c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);

                    // save updated swarm
                    vector<int> aux_swarm;
                    int aux_size;
                    MPI_Recv(&aux_size, 1, MPI_INT, TRACKER_RANK, DOWN_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                    swarm.clear();
                    for (int i = 0; i < aux_size; i++) {
                        int peer;
                        MPI_Recv(&peer, 1, MPI_INT, TRACKER_RANK, DOWN_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                        swarm.push_back(peer);
                    }

                    swarm_size = aux_size;
                    downloadedSegments = 0;
                }
            }

            // create file and add to owned
            pthread_mutex_lock(&file_mutex);
            File new_file;
            new_file.name = file;
            new_file.num_segments = hashes.size();
            new_file.segment_hashes = hashes;
            peer_data.owned_files.push_back(new_file);
            pthread_mutex_unlock(&file_mutex);

            // tell tracker that the file was downloaded
            int msg_type = 2;
            MPI_Send(&msg_type, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);
            MPI_Send(file.c_str(), MAX_FILENAME, MPI_CHAR, TRACKER_RANK, 0, MPI_COMM_WORLD);
            downloadedFiles++;

            // log downloaded file
            ofstream out_file("client" + to_string(rank) + "_" + file);
            if (!out_file.is_open()) {
                exit(EXIT_FAILURE);
            }

            for (string hash : hashes) {
                out_file << hash << "\n";
            }

            out_file.close();
        }

        // Check if all files were downloaded
        int size = peer_data.desired_files.size();
        if (downloadedFiles == size) {
            downloading = false;
        }
    }

    // Send termination signal to tracker
    int msg_type = 3;
    MPI_Send(&msg_type, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD);

    pthread_exit(NULL);
}

void *upload_thread_func(void *arg) {
    MPI_Status status;
    int use_count = 0;
    bool running = true;

    while (running) {
        char hash[HASH_SIZE + 1];
        memset(hash, 0, HASH_SIZE + 1);
        MPI_Recv(hash, HASH_SIZE, MPI_CHAR, MPI_ANY_SOURCE, UP_TAG, MPI_COMM_WORLD, &status);

        // check for termination signal
        if (strcmp(hash, "TERMINATE") == 0) {
            running = false;
            break;
        }

        if (strcmp(hash, "USAGE") == 0) {
            pthread_mutex_lock(&upload_mutex);
            MPI_Send(&use_count, 1, MPI_INT, status.MPI_SOURCE, DOWN_TAG, MPI_COMM_WORLD);
            pthread_mutex_unlock(&upload_mutex);
        }

        bool found = false;
        pthread_mutex_lock(&upload_mutex);
        for (File file : peer_data.owned_files) {
            if (find(file.segment_hashes.begin(), file.segment_hashes.end(), hash) != file.segment_hashes.end()) {
                int ack = 1;
                MPI_Send(&ack, 1, MPI_INT, status.MPI_SOURCE, DOWN_TAG, MPI_COMM_WORLD);
                use_count++;
                found = true;
                break;
            }
        }
        pthread_mutex_unlock(&upload_mutex);

        if (!found) {
            int ack = 0;
            MPI_Send(&ack, 1, MPI_INT, status.MPI_SOURCE, DOWN_TAG, MPI_COMM_WORLD);
        }
    }

    pthread_exit(NULL);
}


void tracker(int numtasks, int rank) {
    MPI_Status status;
    unordered_map<string, vector<string>> file_hashes;
    unordered_map<string, vector<pair<int, string>>> file_swarms;

    // receive file data from each client
    for (int i = 1; i < numtasks; i++) {
        int num_files;
        MPI_Recv(&num_files, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        for (int j = 0; j < num_files; j++) {
            // receive filename
            char file_name[MAX_FILENAME + 1];
            memset(file_name, 0, MAX_FILENAME + 1);
            MPI_Recv(file_name, MAX_FILENAME, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // receive number of hash segmnets
            int num_segments;
            MPI_Recv(&num_segments, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            for (int k = 0; k < num_segments; k++) {
                char hash[HASH_SIZE + 1];
                memset(hash, 0, HASH_SIZE + 1);
                MPI_Recv(hash, HASH_SIZE, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                
                // add hash to file if no dupe
                string hash_str(hash);
                if (find(file_hashes[file_name].begin(), file_hashes[file_name].end(), hash_str) == file_hashes[file_name].end()) {
                    file_hashes[file_name].push_back(hash_str);
                }
            }

            // add client to swarm of file
            file_swarms[file_name].push_back(make_pair(i, "seeding"));
        }
    }

    // send ACK to all peers to indicate they can start communication
    for (int i = 1; i < numtasks; i++) {
        int ack = 1;
        MPI_Send(&ack, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
    }

    // getting messages from clients
    int clients = numtasks - 1;
    bool running = true;
    while (running) {
        int msg_type;
        MPI_Recv(&msg_type, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        int peer_rank = status.MPI_SOURCE;

        if (msg_type == FILE_REQ) {
            // receive name of file
            char file_name[MAX_FILENAME + 1];
            memset(file_name, 0, MAX_FILENAME + 1);
            MPI_Recv(file_name, MAX_FILENAME, MPI_CHAR, peer_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // find swarm and send
            if (file_swarms.find(file_name) != file_swarms.end()) {
                vector<int> sources;
                for (auto peer : file_swarms[file_name]) {
                    sources.push_back(peer.first);
                }
                
                int swarm_size = sources.size();
                MPI_Send(&swarm_size, 1, MPI_INT, peer_rank, DOWN_TAG, MPI_COMM_WORLD);
                for (int source : sources) {
                    MPI_Send(&source, 1, MPI_INT, peer_rank, DOWN_TAG, MPI_COMM_WORLD);
                }
                
                // send hashes
                int num_hashes = file_hashes[file_name].size();
                MPI_Send(&num_hashes, 1, MPI_INT, peer_rank, DOWN_TAG, MPI_COMM_WORLD);

                for (auto hash : file_hashes[file_name]) {
                    MPI_Send(hash.c_str(), HASH_SIZE, MPI_CHAR, peer_rank, DOWN_TAG, MPI_COMM_WORLD);
                }
            }

            file_swarms[file_name].push_back(make_pair(peer_rank, "peering"));
        } else if (msg_type == SEED_REQ) {
            // add client to swarm
            char file_name[MAX_FILENAME + 1];
            memset(file_name, 0, MAX_FILENAME + 1);
            MPI_Recv(file_name, MAX_FILENAME, MPI_CHAR, peer_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // change status to seeding
            for (auto file : file_swarms[file_name]) {
                if (file.first == peer_rank) {
                    file.second = "seeding";
                    break;
                }
            }
        } else if (msg_type == FINISH_REQ) {
            // client disconnected
            clients--;
            if (clients == 0) {
                // send termination signal to all peers
                for (int i = 1; i < numtasks; i++) {
                    char terminate_hash[HASH_SIZE + 1] = "TERMINATE";
                    MPI_Send(terminate_hash, HASH_SIZE, MPI_CHAR, i, UP_TAG, MPI_COMM_WORLD);
                }

                running = false;
            }
        } else if (msg_type == UPDATE_REQ) {
            // receive name of file
            char file_name[MAX_FILENAME + 1];
            memset(file_name, 0, MAX_FILENAME + 1);
            MPI_Recv(file_name, MAX_FILENAME, MPI_CHAR, peer_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // find swarm and send
            if (file_swarms.find(file_name) != file_swarms.end()) {
                vector<int> sources;
                for (auto peer : file_swarms[file_name]) {
                    sources.push_back(peer.first);
                }
                
                int swarm_size = sources.size();
                MPI_Send(&swarm_size, 1, MPI_INT, peer_rank, DOWN_TAG, MPI_COMM_WORLD);
                for (int source : sources) {
                    MPI_Send(&source, 1, MPI_INT, peer_rank, DOWN_TAG, MPI_COMM_WORLD);
                }
            }
        }
    }
}

void peer(int numtasks, int rank) {
    pthread_t download_thread;
    pthread_t upload_thread;
    void *status;
    int r;
    srand(time(NULL));

    // parse input file and send hashes to tracker
    peer_data = parse_input_file(rank);

    // receive acknowledgment from tracker
    int ack;
    MPI_Recv(&ack, 1, MPI_INT, TRACKER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    // create threads
    r = pthread_create(&download_thread, NULL, download_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_create(&upload_thread, NULL, upload_thread_func, (void *) &rank);
    if (r) {
        printf("Eroare la crearea thread-ului de upload\n");
        exit(-1);
    }

    r = pthread_join(download_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de download\n");
        exit(-1);
    }

    r = pthread_join(upload_thread, &status);
    if (r) {
        printf("Eroare la asteptarea thread-ului de upload\n");
        exit(-1);
    }
}
 
int main (int argc, char *argv[]) {
    int numtasks, rank;
 
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        fprintf(stderr, "MPI nu are suport pentru multi-threading\n");
        exit(-1);
    }
    MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    pthread_mutex_init(&upload_mutex, NULL);
    pthread_mutex_init(&file_mutex, NULL);

    if (rank == TRACKER_RANK) {
        tracker(numtasks, rank);
    } else {
        peer(numtasks, rank);
    }

    pthread_mutex_destroy(&upload_mutex);
    pthread_mutex_destroy(&file_mutex);

    MPI_Finalize();
}
