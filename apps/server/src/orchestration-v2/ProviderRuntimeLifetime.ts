import type {
  NodeId,
  OrchestrationV2RuntimeRequest,
  ProviderSessionId,
  ProviderTurnId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Semaphore from "effect/Semaphore";

import { makeKeyedSerialExecutor } from "./KeyedSerialExecutor.ts";

declare const lifetimeBrand: unique symbol;
/** Process-local identity captured by the runtime and each of its consumers. */
export interface ProviderRuntimeLifetime {
  readonly [lifetimeBrand]: true;
}

export interface OwnedRuntimeRequestGroup {
  readonly threadId: ThreadId;
  readonly requestId: RuntimeRequestId;
  readonly nodeId: NodeId;
  readonly providerTurnId: ProviderTurnId | null;
  readonly kind: OrchestrationV2RuntimeRequest["kind"];
  readonly itemIds: ReadonlyArray<TurnItemId>;
}

interface RequestGroup extends OwnedRuntimeRequestGroup {
  readonly capability: OrchestrationV2RuntimeRequest["responseCapability"]["type"];
  cleanupPending: boolean;
}

interface LifetimeState {
  readonly providerSessionId: ProviderSessionId;
  active: boolean;
  ownsProjection: boolean;
  readonly groups: Map<RuntimeRequestId, RequestGroup>;
}

export interface ProviderRuntimeLifetimeLifecycle {
  readonly createLifetime: (
    providerSessionId: ProviderSessionId,
  ) => Effect.Effect<ProviderRuntimeLifetime>;
  readonly activateLifetime: (token: ProviderRuntimeLifetime) => void;
  readonly retireLifetime: (token: ProviderRuntimeLifetime) => void;
  readonly withSessionWrite: <A, E, R>(
    token: ProviderRuntimeLifetime,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A | undefined, E, R>;
  readonly withLifetimeWrite: <A, E, R>(
    token: ProviderRuntimeLifetime,
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E, R>;
  readonly ownsRequest: (
    token: ProviderRuntimeLifetime,
    threadId: ThreadId,
    requestId: RuntimeRequestId,
  ) => Effect.Effect<boolean>;
  readonly getOwnedRequestGroups: (
    token: ProviderRuntimeLifetime,
  ) => ReadonlyArray<OwnedRuntimeRequestGroup>;
  readonly settleOwnedRequestGroups: (
    token: ProviderRuntimeLifetime,
    requestIds: ReadonlyArray<RuntimeRequestId>,
  ) => void;
}

/** Owned by the ingestor service, never a module-global generation registry. */
export const make = Effect.gen(function* () {
  const states = new WeakMap<ProviderRuntimeLifetime, LifetimeState>();
  const owners = new Map<ProviderSessionId, WeakRef<ProviderRuntimeLifetime>>();
  const requestOwners = new Map<RuntimeRequestId, WeakRef<ProviderRuntimeLifetime>>();
  const nodeRequests = new Map<
    NodeId,
    { readonly token: WeakRef<ProviderRuntimeLifetime>; readonly requestId: RuntimeRequestId }
  >();
  const collectedNodes = new FinalizationRegistry<NodeId>((nodeId) => {
    if (nodeRequests.get(nodeId)?.token.deref() === undefined) nodeRequests.delete(nodeId);
  });
  const collectedRequests = new FinalizationRegistry<RuntimeRequestId>((requestId) => {
    if (requestOwners.get(requestId)?.deref() === undefined) requestOwners.delete(requestId);
  });
  const collectedOwners = new FinalizationRegistry<ProviderSessionId>((sessionId) => {
    if (owners.get(sessionId)?.deref() === undefined) owners.delete(sessionId);
  });
  const lanes = yield* makeKeyedSerialExecutor<ProviderSessionId>();
  // SQL identities are global, so admissions on different session lanes must
  // also serialize their collision checks through the corresponding commit.
  const admissions = yield* Semaphore.make(1);
  const state = (token: ProviderRuntimeLifetime) => {
    const value = states.get(token);
    if (value === undefined) throw new Error("Unknown provider runtime lifetime");
    return value;
  };
  const lifecycle: ProviderRuntimeLifetimeLifecycle = {
    createLifetime: (providerSessionId) =>
      Effect.sync(() => {
        const token = {} as ProviderRuntimeLifetime;
        states.set(token, {
          providerSessionId,
          active: false,
          ownsProjection: false,
          groups: new Map(),
        });
        collectedOwners.register(token, providerSessionId);
        return token;
      }),
    activateLifetime: (token) => {
      const next = state(token);
      const previous = owners.get(next.providerSessionId)?.deref();
      if (previous !== undefined && previous !== token) {
        const prior = state(previous);
        prior.ownsProjection = false;
        prior.active = false;
      }
      next.active = true;
      next.ownsProjection = true;
      owners.set(next.providerSessionId, new WeakRef(token));
    },
    retireLifetime: (token) => {
      state(token).active = false;
    },
    withSessionWrite: (token, effect) =>
      lanes.withLock(
        state(token).providerSessionId,
        Effect.suspend(() => (state(token).ownsProjection ? effect : Effect.succeed(undefined))),
      ),
    withLifetimeWrite: (token, effect) => lanes.withLock(state(token).providerSessionId, effect),
    ownsRequest: (token, threadId, requestId) =>
      Effect.sync(() => {
        const current = states.get(token);
        const group = current?.groups.get(requestId);
        return (
          current?.active === true &&
          current.ownsProjection &&
          group?.threadId === threadId &&
          group.capability === "live"
        );
      }),
    getOwnedRequestGroups: (token) =>
      [...state(token).groups.values()].filter((group) => group.cleanupPending),
    settleOwnedRequestGroups: (token, requestIds) => {
      for (const requestId of requestIds) {
        const group = state(token).groups.get(requestId);
        if (group !== undefined) group.cleanupPending = false;
      }
    },
  };
  return {
    lifecycle,
    withAdmission: <A, E, R>(effect: Effect.Effect<A, E, R>) => admissions.withPermit(effect),
    isActive: (token: ProviderRuntimeLifetime) =>
      state(token).active && state(token).ownsProjection,
    sessionId: (token: ProviderRuntimeLifetime) => state(token).providerSessionId,
    requestOwner: (requestId: RuntimeRequestId) => requestOwners.get(requestId)?.deref(),
    nodeRequestId: (nodeId: NodeId) => {
      const reservation = nodeRequests.get(nodeId);
      return reservation?.token.deref() === undefined ? undefined : reservation.requestId;
    },
    group: (token: ProviderRuntimeLifetime, requestId: RuntimeRequestId) =>
      state(token).groups.get(requestId),
    canRegister: (token: ProviderRuntimeLifetime) => state(token).groups.size < 4096,
    register: (token: ProviderRuntimeLifetime, group: Omit<RequestGroup, "cleanupPending">) => {
      const previous = state(token).groups.get(group.requestId);
      if (previous === undefined) {
        requestOwners.set(group.requestId, new WeakRef(token));
        collectedRequests.register(token, group.requestId);
        nodeRequests.set(group.nodeId, { token: new WeakRef(token), requestId: group.requestId });
        collectedNodes.register(token, group.nodeId);
      }
      state(token).groups.set(group.requestId, {
        ...group,
        // Settled metadata remains reachable only through this token, so late
        // siblings cannot reopen it and queued response effects stay fenced.
        cleanupPending: previous?.cleanupPending ?? group.capability === "live",
      });
    },
  };
});
