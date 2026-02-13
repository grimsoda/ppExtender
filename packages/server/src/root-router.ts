import { router } from './trpc';
import { recommenderRouter } from './routers/recommender';

/**
 * Main tRPC app router
 * Combines all sub-routers
 */
export const appRouter = router({
  recommender: recommenderRouter,
});

// Export type for client usage
export type AppRouter = typeof appRouter;
