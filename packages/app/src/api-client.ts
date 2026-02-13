import { QueryClient } from '@tanstack/react-query';

// Create QueryClient for server state management
export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 0, // Refetch immediately when mounted
      retry: 1,
      refetchOnWindowFocus: true,
      refetchOnReconnect: true,
      refetchOnMount: false, // Avoid double-fetching
      gcTime: 1000 * 60 * 5, // Cache for 5 minutes (was cacheTime)
    },
  },
});
