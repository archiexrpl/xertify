# Aviation Proof Example (v1)

This directory defines the Aviation vertical for XERTIFY.

It includes:
- object schema
- event vocabulary
- policy pack
- example proof bundle

## Example Proof

`proof_example.json` is a complete, real proof bundle for an aviation component.

It can be verified without trusting XERTIFY.

### What this proof demonstrates

- The object belongs to the `aviation` vertical
- The verdict was produced by policy `aviation/v1`
- The verdict is reproducible from:
  - object snapshot
  - event history
  - external facts
  - policy hash
- No UI, admin access, or live system trust is required

### How an external system would use this

1. Load the proof bundle
2. Verify hashes (metadata, policy, inputs)
3. Re-evaluate policy if desired
4. Accept or reject the object automatically

This file is intended for:
- auditors
- regulators
- partners
- integrators
