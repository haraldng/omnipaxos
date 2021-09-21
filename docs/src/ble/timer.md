# Tick Timer

The **Ballot Leader Election** does not use timers as there is no available implementation in the official library of Rust. To overcome the use of external libraries the BLE module offers a implementation based on ticks.

If the user has access to the [Kompact](https://github.com/kompics/kompact) framework. Then he would be able to use the representation of timers it comes with.

In the example below we have a timer which repeats the provided function once every 100ms and the heartbeat delay would be 5 ticks, then the heartbeat timeout would happen once every 500ms.

```rust,edition2018,no_run,noplaypen
const BLE_TIMER_TIMEOUT: Duration = Duration::from_millis(100);
self.schedule_periodic(BLE_TIMER_TIMEOUT, BLE_TIMER_TIMEOUT, move |c, _| {
    let leader = c.ble.tick();
    c.ble.get_outgoing_msgs();

    Handled::Ok
}),
```
