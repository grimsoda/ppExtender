import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import SeedInput from '../components/SeedInput';

describe('SeedInput', () => {
  const mockOnSubmit = vi.fn();

  beforeEach(() => {
    mockOnSubmit.mockClear();
  });

  it('should render beatmap_id input field', () => {
    render(<SeedInput onSubmit={mockOnSubmit} />);
    
    expect(screen.getByLabelText(/beatmap id/i)).toBeInTheDocument();
    expect(screen.getByPlaceholderText(/enter beatmap id/i)).toBeInTheDocument();
  });

  it('should render pp range sliders', () => {
    render(<SeedInput onSubmit={mockOnSubmit} />);
    
    expect(screen.getByLabelText(/min pp/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/max pp/i)).toBeInTheDocument();
  });

  it('should render mod checkboxes', () => {
    render(<SeedInput onSubmit={mockOnSubmit} />);
    
    expect(screen.getByLabelText(/hd/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/dt/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/hr/i)).toBeInTheDocument();
    expect(screen.getByLabelText(/fl/i)).toBeInTheDocument();
  });

  it('should handle beatmap_id input change', () => {
    render(<SeedInput onSubmit={mockOnSubmit} />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });
    
    expect(input).toHaveValue('12345');
  });

  it('should handle pp range slider changes', () => {
    render(<SeedInput onSubmit={mockOnSubmit} />);
    
    const minPpSlider = screen.getByLabelText(/min pp/i);
    const maxPpSlider = screen.getByLabelText(/max pp/i);
    
    fireEvent.change(minPpSlider, { target: { value: '200' } });
    fireEvent.change(maxPpSlider, { target: { value: '400' } });
    
    expect(minPpSlider).toHaveValue('200');
    expect(maxPpSlider).toHaveValue('400');
  });

  it('should handle mod checkbox toggles', () => {
    render(<SeedInput onSubmit={mockOnSubmit} />);
    
    const hdCheckbox = screen.getByLabelText(/hd/i);
    const dtCheckbox = screen.getByLabelText(/dt/i);
    
    fireEvent.click(hdCheckbox);
    fireEvent.click(dtCheckbox);
    
    expect(hdCheckbox).toBeChecked();
    expect(dtCheckbox).toBeChecked();
  });

  it('should handle form submission with all values', async () => {
    render(<SeedInput onSubmit={mockOnSubmit} />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: '12345' } });
    
    const minPpSlider = screen.getByLabelText(/min pp/i);
    fireEvent.change(minPpSlider, { target: { value: '200' } });
    
    const hdCheckbox = screen.getByLabelText(/hd/i);
    fireEvent.click(hdCheckbox);
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    await waitFor(() => {
      expect(mockOnSubmit).toHaveBeenCalledWith({
        beatmapId: '12345',
        minPp: 200,
        maxPp: 500,
        mods: ['HD'],
      });
    });
  });

  it('should show validation error for empty beatmap_id', async () => {
    render(<SeedInput onSubmit={mockOnSubmit} />);
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    await waitFor(() => {
      expect(screen.getByText(/beatmap id is required/i)).toBeInTheDocument();
    });
    
    expect(mockOnSubmit).not.toHaveBeenCalled();
  });

  it('should show validation error for invalid beatmap_id format', async () => {
    render(<SeedInput onSubmit={mockOnSubmit} />);
    
    const input = screen.getByLabelText(/beatmap id/i);
    fireEvent.change(input, { target: { value: 'abc' } });
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    fireEvent.click(submitButton);
    
    await waitFor(() => {
      expect(screen.getByText(/invalid beatmap id/i)).toBeInTheDocument();
    });
  });

  it('should disable submit button when loading', () => {
    render(<SeedInput onSubmit={mockOnSubmit} isLoading={true} />);
    
    const submitButton = screen.getByRole('button', { name: /submit/i });
    expect(submitButton).toBeDisabled();
    expect(screen.getByText(/loading/i)).toBeInTheDocument();
  });
});
