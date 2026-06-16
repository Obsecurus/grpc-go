## What & Why

The `lbPicker` in `balancer/grpclb/grpclb_picker.go` maintains a `serverListNext` index that round-robins across the full server list (including drop entries). When `regeneratePicker` in `balancer/grpclb/grpclb.go` was called due to a subchannel state change (e.g., a backend becoming ready), it always created a new `lbPicker` with `serverListNext = 0`, resetting the drop sequence. This broke the deterministic alternating drop pattern that `TestDropRequest` relies on, causing the test to see non-drop RPCs where drops were expected.

The fix preserves the drop index from the previous picker when regenerating due to a subchannel state change, and only resets it to zero when a new server list is received from the remote balancer.

## How

1. **`balancer/grpclb/grpclb.go` — `regeneratePicker`**: Added a `resetDropIndex bool` parameter. When `false`, the function reads `serverListNext` from the current `lb.picker` (if it is an `*lbPicker`) and initializes the new picker with that preserved index. When `true`, the new picker starts at index 0.

2. **`balancer/grpclb/grpclb.go` — `HandleSubConnStateChange`**: Updated the call to `lb.regeneratePicker(false)` so the drop index is preserved across subchannel state transitions.

3. **`balancer/grpclb/grpclb_remote_balancer.go` — `processServerList`**: Updated the call to `lb.regeneratePicker(true)` so the drop index resets whenever a new server list arrives from the load balancer.

## How verified

- `go build ./balancer/grpclb/...` — clean build, no errors.
- `go test ./balancer/grpclb/ -run TestDropRequest -v -timeout 60s` — previously failing test now passes.
- `go test ./balancer/grpclb/ -timeout 120s` — all 13 tests in the package pass.

## Deferred / follow-up

None.
