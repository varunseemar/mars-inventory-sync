# MARS Cosmetics: Order & Inventory Sync Service (Design)

Design for a service that collects orders from 4 sales channels (website, Nykaa, Flipkart, Blinkit),
keeps channel stock in sync with one warehouse without overselling, and sends confirmed orders to
Microsoft Business Central.

| File | What |
|---|---|
| [DESIGN.md](DESIGN.md) | The design doc (architecture, data model, concurrency, failure handling, trade-offs, assumptions) |
| [diagrams/architecture.png](diagrams/architecture.png) | Architecture diagram ([Mermaid source](diagrams/architecture.mmd)) |
| [pseudocode/reserve_stock.md](pseudocode/reserve_stock.md) | Pseudo-code for the stock reservation step |

Stack: Node.js (Express) · PostgreSQL · Redis (BullMQ) · AWS ECS Fargate.
