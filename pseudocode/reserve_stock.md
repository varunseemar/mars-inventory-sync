# Pseudo-code: stock reservation (Question 4)

Runs inside the `ingest` worker (Node.js, `pg` driver). One order = one Postgres transaction.

`available = on_hand - reserved`

## 1. New order (ORDER_CREATED)

```js
async function handleOrderCreated(order) {           // order is already normalized + Zod-validated
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    // 1. Dedup: (channel, external_order_id) is UNIQUE
    const inserted = await client.query(
      `INSERT INTO orders (id, channel, external_order_id, status, erp_status, event_time, raw_payload)
       VALUES ($1, $2, $3, 'NEW', NULL, $4, $5)          -- erp_status set only once reserved
       ON CONFLICT (channel, external_order_id) DO NOTHING
       RETURNING id`,
      [uuid(), order.channel, order.externalOrderId, order.eventTime, order.raw]);

    if (inserted.rowCount === 0) {
      // Already seen: duplicate webhook, poll overlap, or the cancel arrived first.
      await client.query('ROLLBACK');
      // If this is a retry of a job that committed but crashed before step 5,
      // re-queue the side effects. Safe: jobIds dedupe and pushes send absolute qty.
      await requeueSideEffectsIfReserved(order.channel, order.externalOrderId);
      return 'DUPLICATE_IGNORED';
    }
    const orderId = inserted.rows[0].id;
    await insertOrderItems(client, orderId, order.items);

    // 2. Reserve each SKU. Sorted by SKU so two orders always lock rows in the
    //    same order -> no deadlocks between multi-item orders.
    const items = mergeSameSku(order.items).sort((a, b) => a.sku.localeCompare(b.sku));

    for (const item of items) {
      const res = await client.query(
        `UPDATE inventory
            SET reserved = reserved + $2, updated_at = now()
          WHERE sku = $1 AND on_hand - reserved >= $2
          RETURNING on_hand - reserved AS available_after`,
        [item.sku, item.qty]);

      if (res.rowCount === 0) {
        // Not enough stock (or unknown SKU). Undo everything for this order.
        await client.query('ROLLBACK');
        await markRejected(order, item.sku);           // separate small transaction:
                                                       // order row with status REJECTED_OOS
        await enqueueStockPush(item.sku, { urgent: true }); // push 0 to every channel now
        await notifyChannelToCancel(order);            // channel-specific cancel API / ops task
        return 'REJECTED_OOS';
      }

      // 3. Audit trail, same transaction
      await client.query(
        `INSERT INTO stock_movements (sku, change_on_hand, change_reserved, reason, order_id, available_after)
         VALUES ($1, 0, $2, 'RESERVE', $3, $4)`,
        [item.sku, item.qty, orderId, res.rows[0].available_after]);
    }

    // 4. Order is confirmed. ERP status is set in the SAME transaction,
    //    so the order can never be "confirmed but forgotten by the ERP sync".
    await client.query(
      `UPDATE orders SET status = 'RESERVED', erp_status = 'PENDING' WHERE id = $1`, [orderId]);

    await client.query('COMMIT');
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;                                         // BullMQ retries the ingest job
  } finally {
    client.release();
  }

  // 5. After commit: queue side effects. If this fails, the ingest job throws and
  //    BullMQ retries it -> the duplicate branch above re-queues them. The ERP sweep
  //    cron also picks up any order still at erp_status = 'PENDING'.
  await erpQueue.add('send', { orderId }, { jobId: `erp:${orderId}` });
  for (const item of order.items) await enqueueStockPush(item.sku);
  return 'RESERVED';
}
```

## 2. Cancel / return (ORDER_CANCELLED, ORDER_RETURNED)

```js
async function handleOrderCancelled(event) {
  BEGIN
    // Lock the order row so a create and a cancel for the same order can't run at the same time
    order = SELECT * FROM orders WHERE channel = $1 AND external_order_id = $2 FOR UPDATE

    if (!order) {
      // Cancel arrived BEFORE create (Nykaa out-of-order).
      // Store it as CANCELLED with no reservation. When the create arrives later,
      // ON CONFLICT DO NOTHING ignores it, so stock is never reserved.
      INSERT INTO orders (..., status) VALUES (..., 'CANCELLED')
      COMMIT; return
    }

    if (order.status in ['CANCELLED', 'RETURNED', 'REJECTED_OOS']) { COMMIT; return }  // duplicate

    if (order.status === 'RESERVED') {             // not shipped yet -> release reservation
      for item in sortBySku(order.items):
        UPDATE inventory SET reserved = reserved - item.qty WHERE sku = item.sku
        INSERT INTO stock_movements (..., change_reserved = -item.qty, reason = 'RELEASE')
      UPDATE orders SET status = 'CANCELLED'
    }
    // SHIPPED orders: stock comes back only when the warehouse receives the return
    // (RESTOCK: on_hand + qty), see DESIGN.md section 3.
  COMMIT
  enqueue stock push for each SKU
  if order was already sent to ERP: enqueue ERP cancel job
}
```

## Why this is safe when two channels sell the last unit together

- `UPDATE ... WHERE on_hand - reserved >= qty` is a single atomic statement. Postgres takes a
  row lock on that SKU.
- The second transaction waits for the lock. When it gets it, Postgres re-checks the `WHERE`
  against the **new** row (READ COMMITTED re-evaluation), sees `available = 0`, and updates 0 rows.
- `CHECK (reserved <= on_hand)` on the table is a second safety net. Even a bug elsewhere cannot
  make stock negative.
