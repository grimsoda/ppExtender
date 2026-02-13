module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'node',
  roots: ['<rootDir>/src', '<rootDir>/tests'],
  testMatch: ['**/*.test.ts'],
  transform: {
    '^.+\\.tsx?$': 'ts-jest',
  },
  coverageDirectory: 'coverage',
  collectCoverageFrom: [
    'src/**/*.ts',
    '!src/**/*.d.ts',
    '!src/**/*.test.ts',
  ],
  coverageReporters: ['text', 'json', 'html'],
  coverageThreshold: {
    global: {
      lines: 60,
      functions: 60,
      branches: 50,
      statements: 60,
    },
  },
  testTimeout: 30000,
};
