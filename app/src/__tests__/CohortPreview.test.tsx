import { describe, it, expect } from 'vitest';
import { render, screen } from '@testing-library/react';
import CohortPreview from '../components/CohortPreview';

describe('CohortPreview', () => {
  const mockCohortData = {
    size: 150,
    ppDistribution: {
      min: 180,
      max: 450,
      mean: 320,
      median: 310,
    },
    accuracyDistribution: {
      min: 85.5,
      max: 99.8,
      mean: 94.2,
      median: 95.1,
    },
    topPlayers: [
      { userId: 1, username: 'Player1', pp: 450, accuracy: 99.5 },
      { userId: 2, username: 'Player2', pp: 420, accuracy: 98.2 },
      { userId: 3, username: 'Player3', pp: 380, accuracy: 97.8 },
    ],
  };

  it('should render cohort size', () => {
    render(<CohortPreview cohort={mockCohortData} />);
    
    expect(screen.getByText(/cohort size/i)).toBeInTheDocument();
    expect(screen.getByText('150')).toBeInTheDocument();
  });

  it('should render pp distribution statistics', () => {
    render(<CohortPreview cohort={mockCohortData} />);
    
    expect(screen.getByText(/pp distribution/i)).toBeInTheDocument();
    expect(screen.getByText(/min.*180/i)).toBeInTheDocument();
    expect(screen.getByText(/max.*450/i)).toBeInTheDocument();
    expect(screen.getByText(/mean.*320/i)).toBeInTheDocument();
    expect(screen.getByText(/median.*310/i)).toBeInTheDocument();
  });

  it('should render accuracy distribution statistics', () => {
    render(<CohortPreview cohort={mockCohortData} />);
    
    expect(screen.getByText(/accuracy distribution/i)).toBeInTheDocument();
    expect(screen.getByText(/min.*85.5/i)).toBeInTheDocument();
    expect(screen.getByText(/max.*99.8/i)).toBeInTheDocument();
    expect(screen.getByText(/mean.*94.2/i)).toBeInTheDocument();
    expect(screen.getByText(/median.*95.1/i)).toBeInTheDocument();
  });

  it('should render pp histogram chart', () => {
    render(<CohortPreview cohort={mockCohortData} />);
    
    expect(screen.getByTestId('pp-histogram')).toBeInTheDocument();
  });

  it('should render accuracy histogram chart', () => {
    render(<CohortPreview cohort={mockCohortData} />);
    
    expect(screen.getByTestId('accuracy-histogram')).toBeInTheDocument();
  });

  it('should render top players list', () => {
    render(<CohortPreview cohort={mockCohortData} />);
    
    expect(screen.getByText(/top players/i)).toBeInTheDocument();
    expect(screen.getByText('Player1')).toBeInTheDocument();
    expect(screen.getByText('Player2')).toBeInTheDocument();
    expect(screen.getByText('Player3')).toBeInTheDocument();
  });

  it('should display player stats correctly', () => {
    render(<CohortPreview cohort={mockCohortData} />);
    
    expect(screen.getByText(/450.*pp/i)).toBeInTheDocument();
    expect(screen.getByText(/99.5.*%/i)).toBeInTheDocument();
  });

  it('should show empty state when no cohort data', () => {
    render(<CohortPreview cohort={null} />);
    
    expect(screen.getByText(/no cohort data available/i)).toBeInTheDocument();
  });

  it('should show loading state', () => {
    render(<CohortPreview cohort={null} isLoading={true} />);
    
    expect(screen.getByText(/loading cohort data/i)).toBeInTheDocument();
    expect(screen.getByTestId('loading-spinner')).toBeInTheDocument();
  });
});
