import { useCallback, useEffect, useMemo, useState } from 'react';

export type JobLifecycleState =
  | 'CREATED'
  | 'QUEUED'
  | 'RUNNING'
  | 'SUCCEEDED'
  | 'FAILED'
  | 'UNKNOWN';

export interface TrackedJob {
  jobId: string;
  status?: JobLifecycleState | string;
  createdAt?: number;
  updatedAt?: number;
  resultKey?: string | null;
  error?: string | null;
  sourceType?: string | null;
  uploadState?: 'idle' | 'uploading' | 'uploaded' | 'failed';
  lastFetched?: string;
}

const STORAGE_KEY = 'metricfoundry.dashboard.jobs';

function normaliseStatus(status?: string): JobLifecycleState | string | undefined {
  if (!status) return undefined;
  const upper = status.toUpperCase();
  if (['CREATED', 'QUEUED', 'RUNNING', 'SUCCEEDED', 'FAILED'].includes(upper)) {
    return upper as JobLifecycleState;
  }
  return status;
}

function reviveJobs(value: string | null, fallback: TrackedJob[]): TrackedJob[] {
  if (!value) return fallback;
  try {
    const parsed = JSON.parse(value) as TrackedJob[];
    if (!Array.isArray(parsed)) return fallback;
    return parsed.map((job: TrackedJob) => ({
      ...job,
      status: normaliseStatus(job.status),
    }));
  } catch (error) {
    console.warn('Failed to parse persisted jobs', error);
    return fallback;
  }
}

export function usePersistentJobs(initial: TrackedJob[] = []) {
  const [jobs, setJobs] = useState<TrackedJob[]>(() => {
    if (typeof window === 'undefined') return initial;
    return reviveJobs(window.localStorage.getItem(STORAGE_KEY), initial);
  });

  useEffect(() => {
    if (typeof window === 'undefined') return;
    window.localStorage.setItem(STORAGE_KEY, JSON.stringify(jobs));
  }, [jobs]);

  const addJob = useCallback((job: TrackedJob) => {
    setJobs((prev: TrackedJob[]) => {
      const existing = prev.find((item: TrackedJob) => item.jobId === job.jobId);
      if (existing) {
        return prev.map((item: TrackedJob) =>
          item.jobId === job.jobId ? { ...existing, ...job } : item,
        );
      }
      return [{ ...job, status: normaliseStatus(job.status) }, ...prev];
    });
  }, []);

  const updateJob = useCallback((jobId: string, changes: Partial<TrackedJob>) => {
    setJobs((prev: TrackedJob[]) =>
      prev.map((job: TrackedJob) =>
        job.jobId === jobId
          ? {
              ...job,
              ...changes,
              status: normaliseStatus(changes.status ?? job.status),
            }
          : job,
      ),
    );
  }, []);

  const removeJob = useCallback((jobId: string) => {
    setJobs((prev: TrackedJob[]) => prev.filter((job: TrackedJob) => job.jobId !== jobId));
  }, []);

  const sortedJobs = useMemo(
    () => [...jobs].sort((a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0)),
    [jobs],
  );

  return { jobs: sortedJobs, rawJobs: jobs, setJobs, addJob, updateJob, removeJob };
}
