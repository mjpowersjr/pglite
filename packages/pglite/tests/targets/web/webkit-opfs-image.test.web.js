// import { tests } from './base.js'
import { it } from 'vitest'

// Unlike opfs-ahp, the opfs-image VFS only ever holds ONE sync access handle
// open, so it is NOT affected by webkit's ~252 open-handle limit. It is left
// disabled here only until OPFS createSyncAccessHandle support in Playwright's
// Linux webkit build is confirmed in CI; enable once verified.
// tests('webkit', 'opfs-image://base', 'webkit.opfs-image')

it('dummy', () => {})
