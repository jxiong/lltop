/* lltop main.c
 * Copyright 2010 by John L. Hammond <jhammond@tacc.utexas.edu>
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License as
 * published by the Free Software Foundation; either version 2 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301 USA.
 */
#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <signal.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include "lltop.h"
#include "hooks.h"
#include "rbtree.h"

#define MAX_OPS 32

struct op_stat {
	char op[32];
	long count;
};

struct name_stats {
	struct rb_node ns_node;
	long           ns_wr, ns_rd, ns_reqs;
	struct op_stat ops[MAX_OPS];
	int            num_ops;
	char           ns_name[];
};

struct history_node {
	struct rb_node hn_node;
	long           hn_wr, hn_rd, hn_reqs;
	char           hn_key[];
};

struct rb_root name_stats_root  = RB_ROOT;
struct rb_root history_root     = RB_ROOT;
int            name_stats_count = 0;

static struct history_node *
get_history(const char *key)
{
	struct history_node *hn;
	struct rb_node     **link, *parent;

	link   = &history_root.rb_node;
	parent = NULL;

	while (*link != NULL) {
		hn     = rb_entry(*link, struct history_node, hn_node);
		parent = *link;

		int cmp = strcmp(key, hn->hn_key);
		if (cmp < 0)
			link = &((*link)->rb_left);
		else if (cmp > 0)
			link = &((*link)->rb_right);
		else
			return hn;
	}

	hn = alloc(sizeof(*hn) + strlen(key) + 1);
	memset(hn, 0, sizeof(*hn));
	rb_link_node(&hn->hn_node, parent, link);
	rb_insert_color(&hn->hn_node, &history_root);
	strcpy(hn->hn_key, key);
	return hn;
}

static struct name_stats *
get_name_stats(const char *name)
{
	struct name_stats *stats;
	struct rb_node   **link, *parent;

	link   = &name_stats_root.rb_node;
	parent = NULL;

	while (*link != NULL) {
		stats  = rb_entry(*link, struct name_stats, ns_node);
		parent = *link;

		int cmp = strcmp(name, stats->ns_name);
		if (cmp < 0)
			link = &((*link)->rb_left);
		else if (cmp > 0)
			link = &((*link)->rb_right);
		else
			return stats;
	}

	stats = alloc(sizeof(*stats) + strlen(name) + 1);
	memset(stats, 0, sizeof(*stats));
	rb_link_node(&stats->ns_node, parent, link);
	rb_insert_color(&stats->ns_node, &name_stats_root);
	strcpy(stats->ns_name, name);
	name_stats_count++;

	return stats;
}

void
lltop_set_job(const char *host, const char *job)
{
	/* No-op, we aggregate directly by client IP now. */
}

static void
account(const char *addr, long wr, long rd, const char *op, long reqs)
{
	struct name_stats *stats = get_name_stats(addr);
	stats->ns_wr += wr;
	stats->ns_rd += rd;
	if (reqs > 0 && op != NULL) {
		stats->ns_reqs += reqs;
		int i;
		for (i = 0; i < stats->num_ops; i++) {
			if (strcmp(stats->ops[i].op, op) == 0) {
				stats->ops[i].count += reqs;
				return;
			}
		}
		if (stats->num_ops < MAX_OPS) {
			strncpy(stats->ops[stats->num_ops].op, op, sizeof(stats->ops[0].op) - 1);
			stats->ops[stats->num_ops].op[sizeof(stats->ops[0].op) - 1] = '\0';
			stats->ops[stats->num_ops].count                            = reqs;
			stats->num_ops++;
		}
	}
}

static int
name_stats_cmp(const struct name_stats **s1, const struct name_stats **s2)
{
	/* Sort descending by writes, then reads, then requests. */
	/* TODO Make sort rank configurable. */
	long wr = (*s1)->ns_wr - (*s2)->ns_wr;
	if (wr != 0)
		return wr > 0 ? -1 : 1;

	long rd = (*s1)->ns_rd - (*s2)->ns_rd;
	if (rd != 0)
		return rd > 0 ? -1 : 1;

	long reqs = (*s1)->ns_reqs - (*s2)->ns_reqs;
	if (reqs != 0)
		return reqs > 0 ? -1 : 1;

	return 0;
}

static int
op_stat_cmp(const void *a, const void *b)
{
	long c1 = ((const struct op_stat *)a)->count;
	long c2 = ((const struct op_stat *)b)->count;
	return (c1 > c2) ? -1 : ((c1 < c2) ? 1 : 0);
}

static void
account_diff_single(const char *server, const char *target, const char *addr, const char *op,
		    long curr_val)
{
	char key[MAXNAME * 4];
	snprintf(key, sizeof(key), "%s:%s:%s:%s", server, target, addr, op);
	struct history_node *hn = get_history(key);

	long                 diff     = 0;
	int                  is_first = (hn->hn_wr == 0 && hn->hn_rd == 0 && hn->hn_reqs == 0);

	if (!is_first) {
		diff = curr_val - hn->hn_wr;
	}
	hn->hn_wr = curr_val;

	if (diff < 0)
		diff = 0;

	if (is_first)
		return;

	if (diff > 0) {
		if (strcmp(op, "write_bytes") == 0)
			account(addr, diff, 0, NULL, 0);
		else if (strcmp(op, "read_bytes") == 0)
			account(addr, 0, diff, NULL, 0);
		else if (strncmp(op, "req_", 4) == 0)
			account(addr, 0, 0, op + 4, diff);
	}
}

int
main(int argc, char *argv[])
{
	char **serv_list  = NULL;
	int    serv_count = 0;
	if (lltop_config(argc, argv, &serv_list, &serv_count) < 0)
		FATAL("lltop_config() failed\n");

	close(0);
	open("/dev/null", O_RDONLY);

	int first_iteration = 1;

	while (1) {
		rb_destroy(&name_stats_root, offsetof(struct name_stats, ns_node), free);
		name_stats_count = 0;

		if (lltop_job_map != NULL && (*lltop_job_map)() < 0)
			ERROR("cannot get job map: %m\n");

		int fdv[2];
		if (pipe(fdv) < 0)
			FATAL("cannot create pipe: %m\n");

		int i;
		for (i = 0; i < serv_count; i++) {
			pid_t pid = fork();
			if (pid < 0) {
				ERROR("cannot fork: %m\n");
			} else if (pid == 0) {
				close(fdv[0]);
				dup2(fdv[1], 1);
				close(fdv[1]);
				char cmd[2048];
				snprintf(
				    cmd, sizeof(cmd),
				    "%s -o BatchMode=yes -o StrictHostKeyChecking=no %s \"curl -s "
				    "http://localhost:32221/metrics\" | sed \"s/^/%s /\"",
				    lltop_ssh_path, serv_list[i], serv_list[i]);
				execl("/bin/sh", "sh", "-c", cmd, (char *)NULL);
				exit(1);
			}
		}
		close(fdv[1]);

		FILE  *stats_pipe = fdopen(fdv[0], "r");
		char  *line       = NULL;
		size_t line_size  = 0;

		while (getline(&line, &line_size, stats_pipe) >= 0) {
			char server[MAXNAME + 1], rest[4096];
			if (sscanf(line, "%1024s %4095[^\n]", server, rest) != 2)
				continue;

			if (rest[0] == '#')
				continue;

			int is_bytes_total =
			    (strncmp(rest, "lustre_client_export_bytes_total", 32) == 0);
			int is_stats = (strncmp(rest, "lustre_client_export_stats", 26) == 0);
			if (!is_bytes_total && !is_stats)
				continue;

			char *client_start = strstr(rest, "nid=\"");
			if (!client_start)
				continue;
			client_start += 5;
			char *client_end = strchr(client_start, '"');
			if (!client_end)
				continue;

			char   client_buf[MAXNAME];
			size_t client_len = client_end - client_start;
			if (client_len >= sizeof(client_buf))
				client_len = sizeof(client_buf) - 1;
			memcpy(client_buf, client_start, client_len);
			client_buf[client_len] = '\0';

			char  op_buf[256] = "unknown";
			char *op_start    = strstr(rest, "name=\"");
			if (op_start) {
				op_start += 6;
				char *op_end = strchr(op_start, '"');
				if (op_end) {
					size_t op_len = op_end - op_start;
					if (op_len >= sizeof(op_buf))
						op_len = sizeof(op_buf) - 1;
					memcpy(op_buf, op_start, op_len);
					op_buf[op_len] = '\0';
				}
			} else {
				if (strstr(rest, "write_bytes"))
					strcpy(op_buf, "write_bytes");
				else if (strstr(rest, "read_bytes"))
					strcpy(op_buf, "read_bytes");
			}

			char  target_buf[256] = "unknown";
			char *target_start    = strstr(rest, "target=\"");
			if (target_start) {
				target_start += 8;
				char *target_end = strchr(target_start, '"');
				if (target_end) {
					size_t target_len = target_end - target_start;
					if (target_len >= sizeof(target_buf))
						target_len = sizeof(target_buf) - 1;
					memcpy(target_buf, target_start, target_len);
					target_buf[target_len] = '\0';
				}
			}

			char *val_ptr = strrchr(rest, '}');
			if (!val_ptr)
				val_ptr = rest;
			else
				val_ptr++;

			while (*val_ptr == ' ' || *val_ptr == '\t')
				val_ptr++;
			if (*val_ptr == '\0')
				continue;
			long val = atol(val_ptr);

			char addr[MAXNAME];
			strcpy(addr, client_buf);
			chop(addr, '@');

			if (is_bytes_total) {
				if (strcmp(op_buf, "write_bytes") == 0 ||
				    strcmp(op_buf, "read_bytes") == 0) {
					account_diff_single(server, target_buf, addr, op_buf, val);
				}
			} else if (is_stats) {
				if (strcmp(op_buf, "write_bytes") != 0 &&
				    strcmp(op_buf, "read_bytes") != 0 &&
				    strcmp(op_buf, "ping") != 0) {
					char req_op[300];
					snprintf(req_op, sizeof(req_op), "req_%s", op_buf);
					account_diff_single(server, target_buf, addr, req_op, val);
				}
			}
		}
		free(line);
		fclose(stats_pipe);

		while (waitpid(-1, NULL, WNOHANG) > 0)
			;

		if (!first_iteration) {
			struct name_stats **stats_vec;
			stats_vec = alloc(name_stats_count * sizeof(struct name_stats *));

			i = 0;
			struct rb_node *node;
			for (node = rb_first(&name_stats_root); node != NULL; node = rb_next(node))
				stats_vec[i++] = rb_entry(node, struct name_stats, ns_node);

			qsort(stats_vec, name_stats_count, sizeof(struct name_stats *),
			      (int (*)(const void *, const void *))&name_stats_cmp);

			printf("\033[H\033[J");
			lltop_print_header(stdout);

			for (i = 0; i < name_stats_count; i++) {
				struct name_stats *s = stats_vec[i];

				qsort(s->ops, s->num_ops, sizeof(struct op_stat), op_stat_cmp);

				char top_ops_str[128] = "";
				int  j, added = 0;
				for (j = 0; j < s->num_ops && added < 3; j++) {
					if (s->ops[j].count < lltop_threshold)
						break; /* Since it is sorted, remaining are also <
							  threshold */
					char buf[64];
					snprintf(buf, sizeof(buf), "%s%s:%ld",
						 (added == 0) ? "" : ",",
						 s->ops[j].op, s->ops[j].count);
					strcat(top_ops_str, buf);
					added++;
				}
				if (top_ops_str[0] == '\0')
					strcpy(top_ops_str, "-");

				lltop_print_name_stats(stdout, s->ns_name, s->ns_wr, s->ns_rd,
						       top_ops_str);
			}
			free(stats_vec);
			fflush(stdout);

			sleep(lltop_intvl);
		} else {
			first_iteration = 0;
		}
	}

	return 0;
}
