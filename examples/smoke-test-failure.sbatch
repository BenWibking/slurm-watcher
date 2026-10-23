#!/bin/bash
#SBATCH --job-name=slurm-watcher-smoke
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --time=00:02:00
#SBATCH --output=slurm-watcher-smoke-%j.out
#SBATCH --error=slurm-watcher-smoke-%j.err

set -u

echo "slurm-watcher smoke test started"
echo "job_id=${SLURM_JOB_ID:-unset}"
echo "host=$(hostname)"
echo "start_time=$(date --iso-8601=seconds)"

sleep 10

echo "slurm-watcher smoke test: intentional failure" >&2
exit 42
