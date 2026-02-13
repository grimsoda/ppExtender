import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import SeedInput from '../components/SeedInput';

vi.mock('../api-hooks', () => ({
  useGetCohort: vi.fn(() => ({ isLoading: false, data: null, error: null })),
  useGetBeatmapPlays: vi.fn(() => ({ isLoading: false, data: null, error: null })),
  useGetRecommendations: vi.fn(() => ({ isLoading: false, data: null, error: null })),
}));

const createWrapper = () => {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
    },
  });
  return ({ children }: { children: React.ReactNode }) => (
    <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
  );
};

describe('SeedInput', () => {
  const mockOnCohortData = vi.fn();
  const mockOnAllPlays = vi.fn();
  const mockOnRecommendations = vi.fn();
  const mockOnSubmitStart = vi.fn();

  beforeEach(() => {
    mockOnCohortData.mockClear();
    mockOnAllPlays.mockClear();
    mockOnRecommendations.mockClear();
    mockOnSubmitStart.mockClear();
  });

  it('should render beatmap_id input field', () => {
    render(<SeedInput onCohortData={mockOnCohortData} />, { wrapper: createWrapper() });

    expect(screen.getByLabelText(/beatmap id/i)).toBeInTheDocument();
    expect(screen.getByPlaceholderText(/enter beatmap id/i)).toBeInTheDocument();
  });

  it('should render pp range input fields', () => {
    render(<SeedInput onCohortData={mockOnCohortData} />, { wrapper: createWrapper() });

    expect(screen.getByLabelText(/min pp/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/max pp/i)).toBeInTheDocument();
  });

  it('should render mod checkboxes', () => {
    render(<SeedInput onCohortData={mockOnCohortData} />, { wrapper: createWrapper() });

    expect(screen.getByLabelText(/hd/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/dt/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/hr/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/fl/i)).toBeInTheDocument();
  });

  it('should handle beatmap_id input change', () => {
    render(<SeedInput onCohortData={mockOnCohortData} />, { wrapper: createWrapper() });

    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });

    expect(input).toHaveValue('12345');
  });

  it('should handle pp range input changes', () => {
    render(<SeedInput onCohortData={mockOnCohortData} />, { wrapper: createWrapper() });

    const minPpInput = screen.getByLabelText(/min pp/i);
    const maxPpInput = screen.getByLabelText(/max pp/i);

    fireEvent.change(minPpInput, { target: { value: '200' } });
    fireEvent.change(maxPpInput, { target: { value: '400' } });

    expect(minPpInput).toHaveValue(200);
    expect(maxPpInput).toHaveValue(400);
  });

  it('should handle mod checkbox toggles', () => {
    render(<SeedInput onCohortData={mockOnCohortData} />, { wrapper: createWrapper() });

    const hdCheckbox = screen.getByLabelText(/hd/i);
    const dtCheckbox = screen.getByLabelText(/dt/i);

    fireEvent.click(hdCheckbox);
    fireEvent.click(dtCheckbox);

    expect(hdCheckbox).toBeChecked();
    expect(dtCheckbox).toBeChecked();
  });

  it('should handle form submission', async () => {
    render(
      <SeedInput
        onSubmitStart={mockOnSubmitStart}
        onCohortData={mockOnCohortData}
      />,
      { wrapper: createWrapper() }
    );

    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });

    const minPpInput = screen.getByLabelText(/min pp/i);
    fireEvent.change(minPpInput, { target: { value: '200' } });

    const hdCheckbox = screen.getByLabelText(/hd/i);
    fireEvent.click(hdCheckbox);

    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);

    await waitFor(() => {
      expect(mockOnSubmitStart).toHaveBeenCalled();
    });
  });

  it('should show validation error for empty beatmap_id', async () => {
    render(<SeedInput onCohortData={mockOnCohortData} />, { wrapper: createWrapper() });

    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);

    await waitFor(() => {
      expect(screen.getByText(/beatmap id is required/i)).toBeInTheDocument();
    });

    expect(mockOnSubmitStart).not.toHaveBeenCalled();
  });

  it('should show validation error for invalid beatmap_id format', async () => {
    render(<SeedInput onCohortData={mockOnCohortData} />, { wrapper: createWrapper() });

    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: 'abc' } });

    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);

    await waitFor(() => {
      expect(screen.getByText(/invalid beatmap id/i)).toBeInTheDocument();
    });
  });
});
