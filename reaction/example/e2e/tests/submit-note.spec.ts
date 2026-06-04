import { test, expect } from '@playwright/test';

import {
  byId,
  fillField,
  readSemanticValue,
  waitForFlutter,
} from '../helpers';

test('submit a note and see it appear in the list', async ({ page }) => {
  await page.goto('/');
  await waitForFlutter(page);
  await page.waitForSelector(byId('login-username'), { timeout: 30_000 });

  // Log in as alice (editor-west) so submit succeeds. The username field
  // already defaults to `alice`; click the login button to authenticate.
  await page.locator(byId('login-button')).click();

  // The submit-note form should mount; wait for its status node.
  await page.waitForSelector(byId('submit-note'), { timeout: 30_000 });

  // Fill the title field (targets the enabled input; the wrapper's
  // textField flag spawns a phantom disabled one).
  const title = `e2e note ${Date.now()}`;
  await fillField(page, 'submit-note-title', title);
  await page.locator(byId('submit-note-button')).click();

  // Assert the ActionBuilder lifecycle node reports success.
  await expect
    .poll(async () => readSemanticValue(page, 'submit-note'), {
      timeout: 15_000,
    })
    .toBe('success');

  // Assert a note-row carrying our unique title exists. Semantics nodes
  // live under the intentionally-hidden `flt-semantics-host`, so assert the
  // node is ATTACHED (present in the DOM) rather than "visible" — visibility
  // is brittle for an off-screen / opacity-0 host.
  await expect(
    page.locator(byId('note-row'), { hasText: title }),
  ).toBeAttached({ timeout: 15_000 });
});
