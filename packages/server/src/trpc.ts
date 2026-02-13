import { initTRPC } from '@trpc/server';
import { z } from 'zod';

/**
 * tRPC router initialization
 * Follows Task 9 from v2 modernization plan
 * 
 * This sets up the tRPC instance with:
 * - Type-safe procedures
 * - Zod for input validation
 * - Context for database access
 */

const t = initTRPC.create();

// Export the router and procedure helpers
export const router = t.router;
export const publicProcedure = t.procedure;
export const middleware = t.middleware;
export const mergeRouters = t.mergeRouters;

// Re-export types for client usage
export type Router = typeof t.router;
export type Procedure = typeof t.procedure;
