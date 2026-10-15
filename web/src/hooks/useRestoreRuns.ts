/*
 * Copyright (c) 2025-2026, s0up and the autobrr contributors.
 * SPDX-License-Identifier: GPL-2.0-or-later
 */

import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query"

import { useActivityStream } from "@/contexts/SyncStreamContext"
import { api } from "@/lib/api"
import type { RestoreMode, RestoreRun } from "@/types"

const RESTORE_ACTIVE_STATUSES = ["queued", "running"] as const

export function isRestoreActive(run?: Pick<RestoreRun, "status"> | null): boolean {
  return !!run && (RESTORE_ACTIVE_STATUSES as readonly string[]).includes(run.status)
}

export function useRestoreRuns(
  instanceId: number,
  options?: { limit?: number; offset?: number; enabled?: boolean }
) {
  const shouldEnable = (options?.enabled ?? true) && instanceId > 0
  useActivityStream(shouldEnable)

  const limit = options?.limit
  const offset = options?.offset

  return useQuery({
    queryKey: ["instance-restore-runs", instanceId, "runs", limit ?? null, offset ?? null],
    queryFn: () =>
      api.listRestoreRuns(instanceId, {
        ...(limit !== undefined ? { limit } : {}),
        ...(offset !== undefined ? { offset } : {}),
      }),
    enabled: shouldEnable,
    refetchInterval: query => {
      const response = query.state.data
      return response?.runs.some(run => isRestoreActive(run)) ? 500 : false
    },
  })
}

export function useRestoreRun(instanceId: number, restoreRunId?: number | null) {
  const enabled = instanceId > 0 && typeof restoreRunId === "number" && restoreRunId > 0
  useActivityStream(enabled)

  return useQuery({
    queryKey: ["instance-restore-runs", instanceId, "run", restoreRunId ?? null],
    queryFn: () => api.getRestoreRun(instanceId, restoreRunId as number),
    enabled,
    refetchInterval: query => (isRestoreActive(query.state.data) ? 500 : false),
  })
}

export function useRestoreOps(
  instanceId: number,
  restoreRunId?: number | null,
  options?: { enabled?: boolean; refetchInterval?: number | false }
) {
  const enabled = (options?.enabled ?? true) && instanceId > 0 && typeof restoreRunId === "number"

  return useQuery({
    queryKey: ["instance-restore-runs", instanceId, "ops", restoreRunId ?? null],
    queryFn: () => api.listRestoreOps(instanceId, restoreRunId as number),
    enabled,
    refetchInterval: options?.refetchInterval,
  })
}

export function useEnqueueRestore(instanceId: number) {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (payload: {
      runId: number
      mode: RestoreMode
      dryRun: boolean
      excludeHashes?: string[]
      startPaused?: boolean
      skipHashCheck?: boolean
      autoResumeVerified?: boolean
    }) =>
      api.enqueueRestore(instanceId, payload.runId, {
        mode: payload.mode,
        dryRun: payload.dryRun,
        excludeHashes: payload.excludeHashes,
        startPaused: payload.startPaused,
        skipHashCheck: payload.skipHashCheck,
        autoResumeVerified: payload.autoResumeVerified,
      }),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["instance-restore-runs", instanceId] })
    },
  })
}

export function useCancelRestoreRun(instanceId: number) {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (restoreRunId: number) => api.cancelRestoreRun(instanceId, restoreRunId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["instance-restore-runs", instanceId] })
    },
  })
}

export function useResumeRestoreRun(instanceId: number) {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: (restoreRunId: number) => api.resumeRestoreRun(instanceId, restoreRunId),
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ["instance-restore-runs", instanceId] })
    },
  })
}
