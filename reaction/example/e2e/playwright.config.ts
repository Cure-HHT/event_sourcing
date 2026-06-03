import { defineConfig, devices } from '@playwright/test';

// The dart demo server (port 8080) and the `flutter build web` step are
// orchestrated by scripts/run-e2e.sh. Playwright only serves the built
// bundle and runs the specs against Chromium.
export default defineConfig({
  testDir: './tests',
  timeout: 60_000,
  expect: { timeout: 15_000 },
  fullyParallel: false,
  retries: 0,
  reporter: [['list']],
  use: {
    baseURL: 'http://localhost:8000',
    trace: 'on-first-retry',
  },
  projects: [
    { name: 'chromium', use: { ...devices['Desktop Chrome'] } },
  ],
  webServer: {
    command: 'npx --yes serve ../build/web -l 8000 --no-clipboard',
    url: 'http://localhost:8000',
    reuseExistingServer: !process.env.CI,
    timeout: 60_000,
  },
});
