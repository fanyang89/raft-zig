const std = @import("std");
const transport = @import("../transport.zig");

pub const PeerEventQueue = struct {
    const Item = struct {
        event: transport.PeerEvent,
        generation: u64,
    };

    mutex: std.atomic.Mutex = .unlocked,
    items: std.ArrayList(Item) = .empty,
    head: usize = 0,
    max_events: usize,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, max_events: usize) !PeerEventQueue {
        if (max_events == 0) return error.InvalidConfig;
        var self = PeerEventQueue{ .allocator = allocator, .max_events = max_events };
        try self.items.ensureTotalCapacity(allocator, max_events);
        return self;
    }

    pub fn deinit(self: *PeerEventQueue) void {
        self.items.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn push(self: *PeerEventQueue, event: transport.PeerEvent, generation: u64) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        for (self.items.items[self.head..]) |item| {
            if (item.event.peer_id == event.peer_id and item.event.kind == event.kind and item.generation == generation) return;
        }
        if (self.items.items.len - self.head == self.max_events) {
            self.head += 1;
        }
        if (self.head > 0 and self.items.items.len == self.items.capacity) self.compact();
        self.items.appendAssumeCapacity(.{ .event = event, .generation = generation });
    }

    pub fn pop(self: *PeerEventQueue) ?transport.PeerEvent {
        lock(&self.mutex);
        defer self.mutex.unlock();
        if (self.head == self.items.items.len) return null;
        const event = self.items.items[self.head].event;
        self.head += 1;
        if (self.head == self.items.items.len) {
            self.items.clearRetainingCapacity();
            self.head = 0;
        }
        return event;
    }

    pub fn clear(self: *PeerEventQueue) void {
        lock(&self.mutex);
        defer self.mutex.unlock();
        self.items.clearRetainingCapacity();
        self.head = 0;
    }

    fn compact(self: *PeerEventQueue) void {
        const pending = self.items.items.len - self.head;
        std.mem.copyForwards(Item, self.items.items[0..pending], self.items.items[self.head..]);
        self.items.shrinkRetainingCapacity(pending);
        self.head = 0;
    }
};

fn lock(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
}

test "full peer event queue drops oldest and compacts" {
    var queue = try PeerEventQueue.init(std.testing.allocator, 2);
    defer queue.deinit();

    queue.push(.{ .peer_id = 1, .kind = .@"unreachable" }, 1);
    queue.push(.{ .peer_id = 2, .kind = .snapshot_failure }, 1);
    var next_peer_id: u64 = 3;
    while (queue.items.items.len < queue.items.capacity) {
        queue.push(.{ .peer_id = next_peer_id, .kind = .identity_rejected }, 1);
        next_peer_id += 1;
    }
    queue.push(.{ .peer_id = next_peer_id, .kind = .identity_rejected }, 1);

    try std.testing.expectEqual(@as(usize, 0), queue.head);
    try std.testing.expectEqual(@as(usize, 2), queue.items.items.len);
    try std.testing.expectEqual(next_peer_id - 1, queue.pop().?.peer_id);
    try std.testing.expectEqual(next_peer_id, queue.pop().?.peer_id);
    try std.testing.expectEqual(@as(?transport.PeerEvent, null), queue.pop());
}
