import { test, expect, Page } from '@playwright/test';

// Selectors target Flutter's web semantics tree (force-enabled at boot in
// main.dart). Each Flutter Semantics(identifier:) surfaces as a
// `flt-semantics-identifier` attribute under the flt-semantics-host.
const byId = (id: string) => `[flt-semantics-identifier="${id}"]`;

// Wait for CanvasKit to finish booting and the semantics DOM to exist.
async function waitForApp(page: Page) {
  await page.waitForSelector('flt-semantics-host', { timeout: 30_000 });
  await page.waitForSelector(byId('login-username'), { timeout: 30_000 });
}

test('submit a note and see it appear in the list', async ({ page }) => {
  await page.goto('/');
  await waitForApp(page);

  // Log in as alice (editor-west) so submit succeeds.
  await page.locator(byId('login-username')).click();
  await page.keyboard.type('alice');
  await page.locator(byId('login-button')).click();

  // The submit-note form should mount; wait for its status node.
  await page.waitForSelector(byId('submit-note'), { timeout: 30_000 });

  const title = `e2e note ${Date.now()}`;
  await page.locator(byId('submit-note-title')).click();
  await page.keyboard.type(title);
  await page.locator(byId('submit-note-button')).click();

  // Assert the ActionBuilder lifecycle node reports success.
  await expect
    .poll(
      async () =>
        page.locator(byId('submit-note')).getAttribute('aria-valuetext'),
      { timeout: 15_000 },
    )
    .toBe('success');

  // Assert a note-row carrying our unique title exists.
  await expect(
    page.locator(`${byId('note-row')}`, { hasText: title }),
  ).toBeVisible({ timeout: 15_000 });
});
