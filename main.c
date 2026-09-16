/*
 * Multi-stage ping-pong simulation using shared memory and POSIX threads.
 *
 * Parameters (all via command line, no randomness used anywhere):
 *   argv[1] = N            number of worker processes (line length), N >= 2
 *   argv[2] = MAX_BALLS    maximum number of balls that may be in flight
 *   argv[3] = TOTAL_BALLS  total number of balls process 1 will inject
 *   argv[4] = ROUNDS       number of full round trips before process 1
 *                          removes a ball
 *   argv[5] = STEP_MS      (optional) delay in milliseconds between
 *                          simulation steps, default 200
 *
 * Build:
 *   gcc -Wall -Wextra -O2 -pthread -g -o pingpong pingpong.c -lrt
 *
 * Run:
 *   ./pingpong 4 5 10 3
 *
 * While running, press 'q' + Enter in the controller's terminal to quit,
 * or any other character + Enter to trigger the next signal cycle.
 */

#define _GNU_SOURCE
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <pthread.h>
#include <time.h>

#define MAX_N            32
#define MAX_BALLS_LIMIT  256
#define MAX_ROUNDS_TRACK 64
#define FIFO_PATH_LEN    64

#define DIR_FORWARD   1
#define DIR_BACKWARD -1

typedef struct {
    int in_use;
    int position;
    int direction;
    int round_cnt;
    int id;
} ball_t;


typedef struct {
    long count[MAX_ROUNDS_TRACK][2];
} worker_stats_t;


typedef struct {
    pthread_mutex_t lock;

    int n_workers;
    int max_balls;
    int total_balls;
    int rounds_limit;
    int step_ms;

    int injected_count;
    int removed_count;
    int next_ball_id;

    int shutdown;

    ball_t balls[MAX_BALLS_LIMIT];

    worker_stats_t stats[MAX_N + 1];
} shared_data_t;

static shared_data_t *g_shm = NULL;
static const char *g_shm_name = "/pingpong_shm";

static int g_n_workers;
static int g_max_balls;
static int g_total_balls;
static int g_rounds_limit;
static int g_step_ms;

static char g_fifo_paths[MAX_N + 1][FIFO_PATH_LEN];

static pid_t g_worker_pids[MAX_N + 1];
static pid_t g_controller_pid;



static void die(const char *msg) {
    perror(msg);
    exit(EXIT_FAILURE);
}


static void shm_create_and_init(void) {
    shm_unlink(g_shm_name);

    int fd = shm_open(g_shm_name, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (fd < 0) die("shm_open");

    if (ftruncate(fd, sizeof(shared_data_t)) != 0) die("ftruncate");

    void *addr = mmap(NULL, sizeof(shared_data_t),
                       PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (addr == MAP_FAILED) die("mmap");

    close(fd);

    g_shm = (shared_data_t *)addr;
    memset(g_shm, 0, sizeof(shared_data_t));

    pthread_mutexattr_t attr;
    if (pthread_mutexattr_init(&attr) != 0) die("pthread_mutexattr_init");
    if (pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED) != 0)
        die("pthread_mutexattr_setpshared");
    if (pthread_mutex_init(&g_shm->lock, &attr) != 0)
        die("pthread_mutex_init");
    pthread_mutexattr_destroy(&attr);

    g_shm->n_workers    = g_n_workers;
    g_shm->max_balls    = g_max_balls;
    g_shm->total_balls  = g_total_balls;
    g_shm->rounds_limit = g_rounds_limit;
    g_shm->step_ms      = g_step_ms;
    g_shm->injected_count = 0;
    g_shm->removed_count  = 0;
    g_shm->next_ball_id   = 1;
    g_shm->shutdown       = 0;

    for (int i = 0; i < g_max_balls; i++) {
        g_shm->balls[i].in_use = 0;
    }
}

static void shm_destroy(void) {
    if (g_shm != NULL) {
        pthread_mutex_destroy(&g_shm->lock);
        munmap(g_shm, sizeof(shared_data_t));
        g_shm = NULL;
    }
    shm_unlink(g_shm_name);
}

static void fifo_paths_init(void) {
    for (int i = 1; i <= g_n_workers; i++) {
        snprintf(g_fifo_paths[i], FIFO_PATH_LEN, "/tmp/pingpong_stats_%d", i);
    }
}

static void fifo_create_all(void) {
    for (int i = 1; i <= g_n_workers; i++) {
        unlink(g_fifo_paths[i]);
        if (mkfifo(g_fifo_paths[i], 0600) != 0) {
            if (errno != EEXIST) die("mkfifo");
        }
    }
}

static void fifo_remove_all(void) {
    for (int i = 1; i <= g_n_workers; i++) {
        unlink(g_fifo_paths[i]);
    }
}


static int g_my_index;
static volatile sig_atomic_t g_signal_received = 0;

static void sigusr1_handler(int signo) {
    (void)signo;
    g_signal_received = 1;
}

static void worker_install_signal_handler(void) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = sigusr1_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = 0;
    if (sigaction(SIGUSR1, &sa, NULL) != 0) die("sigaction");
}


static void worker_send_stats(int worker_index) {
    worker_stats_t snapshot;

    pthread_mutex_lock(&g_shm->lock);
    snapshot = g_shm->stats[worker_index];
    pthread_mutex_unlock(&g_shm->lock);

    char buf[4096];
    int off = 0;
    off += snprintf(buf + off, sizeof(buf) - off,
                     "STATS process=%d\n", worker_index);

    for (int r = 0; r < MAX_ROUNDS_TRACK; r++) {
        long fwd = snapshot.count[r][0];
        long bwd = snapshot.count[r][1];
        if (fwd == 0 && bwd == 0)
            continue;
        off += snprintf(buf + off, sizeof(buf) - off,
                         "  round=%d forward=%ld backward=%ld\n",
                         r, fwd, bwd);
        if (off >= (int)sizeof(buf) - 64)
            break;
    }
    off += snprintf(buf + off, sizeof(buf) - off, "END\n");

    int fd = open(g_fifo_paths[worker_index], O_WRONLY);
    if (fd < 0) {
        return;
    }
    ssize_t written = write(fd, buf, (size_t)off);
    (void)written;
    close(fd);
}


static void *worker_signal_thread(void *arg) {
    int idx = *(int *)arg;

    for (;;) {
        pthread_mutex_lock(&g_shm->lock);
        int done = g_shm->shutdown;
        pthread_mutex_unlock(&g_shm->lock);
        if (done)   
        {
            break;
        }
        if (g_signal_received)
        {
            g_signal_received = 0;
            worker_send_stats(idx);
        }

        struct timespec ts = { .tv_sec = 0, .tv_nsec = 50 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    return NULL;
}


static void record_pass(int worker_index, int round_cnt, int direction) {
    int r = round_cnt;
    if (r >= MAX_ROUNDS_TRACK)
        r = MAX_ROUNDS_TRACK - 1;
    int d = (direction == DIR_FORWARD) ? 0 : 1;
    g_shm->stats[worker_index].count[r][d]++;
}


static void advance_ball(ball_t *b) {
    int from = b->position;

    if (b->direction == DIR_FORWARD) {
        record_pass(from, b->round_cnt, DIR_FORWARD);
        if (from == g_shm->n_workers) {
            b->direction = DIR_BACKWARD;
            b->position = from - 1;
        } else {
            b->position = from + 1;
        }
    } else {
        record_pass(from, b->round_cnt, DIR_BACKWARD);
        if (from == 1) {
            b->round_cnt++;
            if (b->round_cnt >= g_shm->rounds_limit) {
                b->in_use = 0;
                g_shm->removed_count++;
                return;
            }
            b->direction = DIR_FORWARD;
            b->position = 2;
        } else {
            b->position = from - 1;
        }
    }
}

static int try_inject_ball(void) {
    if (g_shm->injected_count >= g_shm->total_balls)
        return 0;

    for (int i = 0; i < g_shm->max_balls; i++) {
        if (!g_shm->balls[i].in_use) {
            g_shm->balls[i].in_use    = 1;
            g_shm->balls[i].position  = 1;
            g_shm->balls[i].direction = DIR_FORWARD;
            g_shm->balls[i].round_cnt = 0;
            g_shm->balls[i].id        = g_shm->next_ball_id++;
            g_shm->injected_count++;
            return 1;
        }
    }
    return 0;
}


static void *worker_simulation_thread(void *arg) {
    (void)arg;

    for (;;) {
        pthread_mutex_lock(&g_shm->lock);

        if (g_shm->shutdown) {
            pthread_mutex_unlock(&g_shm->lock);
            break;
        }

        for (int i = 0; i < g_shm->max_balls; i++) {
            if (g_shm->balls[i].in_use) {
                advance_ball(&g_shm->balls[i]);
            }
        }

        try_inject_ball();

        int any_active = 0;
        for (int i = 0; i < g_shm->max_balls; i++) {
            if (g_shm->balls[i].in_use) { any_active = 1; break; }
        }
        if (g_shm->injected_count >= g_shm->total_balls && !any_active) {
            g_shm->shutdown = 1;
        }

        pthread_mutex_unlock(&g_shm->lock);

        struct timespec ts;
        ts.tv_sec  = g_shm->step_ms / 1000;
        ts.tv_nsec = (g_shm->step_ms % 1000) * 1000 * 1000;
        nanosleep(&ts, NULL);
    }
    return NULL;
}


static int worker_main(int index) {
    g_my_index = index;

    worker_install_signal_handler();

    pthread_t sig_thread;
    if (pthread_create(&sig_thread, NULL, worker_signal_thread, &g_my_index) != 0)
        die("pthread_create (signal thread)");

    pthread_t sim_thread;
    int have_sim_thread = 0;
    if (index == 1) {
        if (pthread_create(&sim_thread, NULL, worker_simulation_thread, NULL) != 0)
            die("pthread_create (simulation thread)");
        have_sim_thread = 1;
    }

    if (have_sim_thread) {
        pthread_join(sim_thread, NULL);
    } else {
        for (;;) {
            pthread_mutex_lock(&g_shm->lock);
            int done = g_shm->shutdown;
            pthread_mutex_unlock(&g_shm->lock);
            if (done) break;

            struct timespec ts = { .tv_sec = 0, .tv_nsec = 100 * 1000 * 1000 };
            nanosleep(&ts, NULL);
        }
    }

    pthread_join(sig_thread, NULL);
    return 0;
}


static int controller_main(void) {
    int target = 1;

    printf("Controller ready. %d worker process(es) in the chain.\n",
           g_n_workers);
    printf("Press Enter to signal worker #%d and request its statistics.\n",
           target);
    printf("Type 'q' then Enter to quit.\n");
    fflush(stdout);

    char line[16];

    for (;;) {
        pthread_mutex_lock(&g_shm->lock);
        int done = g_shm->shutdown;
        pthread_mutex_unlock(&g_shm->lock);
        if (done) {
            printf("Simulation finished (all balls processed). Exiting controller.\n");
            break;
        }

        if (fgets(line, sizeof(line), stdin) == NULL) {
            break;
        }

        if (line[0] == 'q' || line[0] == 'Q') {
            printf("Controller exiting (simulation continues in background).\n");
            break;
        }

        if (kill(g_worker_pids[target], SIGUSR1) != 0) {
            perror("kill");
        } else {
         
            int fd = open(g_fifo_paths[target], O_RDONLY);
            if (fd >= 0) {
                char buf[4096];
                ssize_t n = read(fd, buf, sizeof(buf) - 1);
                close(fd);
                if (n > 0) {
                    buf[n] = '\0';
                    printf("--- Reply from worker #%d ---\n%s", target, buf);
                } else {
                    printf("(no data received from worker #%d)\n", target);
                }
            } else {
                perror("open fifo");
            }
        }

        target = (target % g_n_workers) + 1;
        printf("Press Enter to signal worker #%d (or 'q' to quit).\n", target);
        fflush(stdout);
    }

    return 0;
}


static void cleanup_all(void) {
    shm_destroy();
    fifo_remove_all();
}


static void print_usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s N MAX_BALLS TOTAL_BALLS ROUNDS [STEP_MS]\n"
        "  N           number of worker processes (2..%d)\n"
        "  MAX_BALLS   maximum balls in flight (1..%d)\n"
        "  TOTAL_BALLS total balls to inject (>= MAX_BALLS recommended)\n"
        "  ROUNDS      full round trips (1->N->1) before process 1 removes a ball\n"
        "  STEP_MS     optional, delay between steps in ms (default 200)\n",
        prog, MAX_N, MAX_BALLS_LIMIT);
}

int main(int argc, char **argv) {
    if (argc < 5 || argc > 6) {
        print_usage(argv[0]);
        return EXIT_FAILURE;
    }

    g_n_workers   = atoi(argv[1]);
    g_max_balls   = atoi(argv[2]);
    g_total_balls = atoi(argv[3]);
    g_rounds_limit = atoi(argv[4]);
    g_step_ms = (argc == 6) ? atoi(argv[5]) : 200;

    if (g_n_workers < 2 || g_n_workers > MAX_N) {
        fprintf(stderr, "Error: N must be between 2 and %d\n", MAX_N);
        return EXIT_FAILURE;
    }
    if (g_max_balls < 1 || g_max_balls > MAX_BALLS_LIMIT) {
        fprintf(stderr, "Error: MAX_BALLS must be between 1 and %d\n", MAX_BALLS_LIMIT);
        return EXIT_FAILURE;
    }
    if (g_total_balls < 1) {
        fprintf(stderr, "Error: TOTAL_BALLS must be >= 1\n");
        return EXIT_FAILURE;
    }
    if (g_rounds_limit < 1 || g_rounds_limit > MAX_ROUNDS_TRACK) {
        fprintf(stderr, "Error: ROUNDS must be between 1 and %d\n", MAX_ROUNDS_TRACK);
        return EXIT_FAILURE;
    }
    if (g_step_ms < 0) {
        fprintf(stderr, "Error: STEP_MS must be >= 0\n");
        return EXIT_FAILURE;
    }

    fifo_paths_init();
    fifo_create_all();
    shm_create_and_init();

    printf("Starting %d worker process(es), MAX_BALLS=%d, TOTAL_BALLS=%d, "
           "ROUNDS=%d, STEP_MS=%d\n",
           g_n_workers, g_max_balls, g_total_balls, g_rounds_limit, g_step_ms);
    fflush(stdout);

    for (int i = 1; i <= g_n_workers; i++) {
        pid_t pid = fork();
        if (pid < 0) die("fork (worker)");
        if (pid == 0) {
            int rc = worker_main(i);
            exit(rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
        }
        g_worker_pids[i] = pid;
    }

    {
        pid_t pid = fork();
        if (pid < 0) die("fork (controller)");
        if (pid == 0) {
            int rc = controller_main();
            exit(rc == 0 ? EXIT_SUCCESS : EXIT_FAILURE);
        }
        g_controller_pid = pid;
    }

   
    for (int i = 1; i <= g_n_workers; i++) {
        waitpid(g_worker_pids[i], NULL, 0);
    }

    pthread_mutex_lock(&g_shm->lock);
    g_shm->shutdown = 1;
    pthread_mutex_unlock(&g_shm->lock);

    kill(g_controller_pid, SIGTERM);
    waitpid(g_controller_pid, NULL, 0);

    printf("All balls processed (%d injected, %d removed). Cleaning up.\n",
           g_shm->injected_count, g_shm->removed_count);

    cleanup_all();
    return EXIT_SUCCESS;
}