# Product Thinking System

Four connected commands for strategic product development.

## The Hierarchy

```
┌────────────────────────────────────────────────────┐
│  PRODUCT VISION                                    │
│  Command: product vision                           │
│  Document: VISION.md                               │
│  Updates: Rarely (pivots only)                     │
│  Question: "What transformation are we enabling?"  │
└───────────────────────┬────────────────────────────┘
                        │
┌───────────────────────▼────────────────────────────┐
│  PRODUCT ROADMAP                                   │
│  Command: product roadmap                          │
│  Document: ROADMAP.md                              │
│  Updates: Periodic (monthly/quarterly)             │
│  Question: "Where are we investing?"               │
└───────────────────────┬────────────────────────────┘
                        │
┌───────────────────────▼────────────────────────────┐
│  PRODUCT HYPOTHESES                                │
│  Command: product hypotheses                       │
│  Document: HYPOTHESES.md                           │
│  Updates: Constantly (living document)             │
│  Question: "What do we believe will work?"         │
└───────────────────────┬────────────────────────────┘
                        │
┌───────────────────────▼────────────────────────────┐
│  PRODUCT ITERATION                                 │
│  Command: product iteration                        │
│  Document: Per-feature output                      │
│  Updates: After each feature                       │
│  Question: "What did we learn? What's next?"       │
└────────────────────────────────────────────────────┘
```

## The Flow

### Starting a Product
1. `product vision` → Create VISION.md
2. `product roadmap` → Create ROADMAP.md with investment areas
3. `product hypotheses` → Generate initial hypotheses from roadmap

### Day-to-Day Development
1. `product hypotheses` → Select hypothesis to test
2. Build feature to test hypothesis
3. `product iteration` → Analyze what we learned
4. Update HYPOTHESES.md with results
5. Repeat

### Periodic Strategy
1. Review HYPOTHESES.md learnings
2. `product roadmap` → Update investment areas based on learnings
3. Rarely: `product vision` → Adjust if fundamental shift needed

## Key Principles (Butterfield + Jobs)

**From Stewart Butterfield:**
- Utility curves: Know where you are on the S-curve
- Sell the transformation, not the saddle
- Beware owner's delusion
- Empathy for small irritations

**From Steve Jobs:**
- Start with desired experience, work backwards
- Deep simplicity > surface minimalism
- Innovation = saying no to 1000 things
- Design is how it works, not how it looks

## Document Locations

```
project/
├── VISION.md        # North star (rarely changes)
├── ROADMAP.md       # Investment areas (periodic updates)
├── HYPOTHESES.md    # Living bets catalog (constant updates)
└── docs/
    └── iterations/  # Optional: archive of iteration outputs
```

## Quick Reference

| I want to... | Command | Updates |
|--------------|---------|---------|
| Define what we're building | `product vision` | VISION.md |
| Plan where to invest | `product roadmap` | ROADMAP.md |
| Decide what to build next | `product hypotheses` | HYPOTHESES.md |
| Learn from completed feature | `product iteration` | HYPOTHESES.md |
