// author: kodeholic (powered by Claude)
//! Benchmark report — terminal output for fanout and conference modes

use crate::media::BenchResult;
use crate::conference::ConferenceResult;
use crate::Args;

// ═══════════════════════════════════════════════════════════
// Fan-out report (existing)
// ═══════════════════════════════════════════════════════════

pub fn print_report(args: &Args, r: &BenchResult) {
    println!();
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║            LIGHT-SFU BENCHMARK REPORT                   ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();
    println!("  label:         {}", args.label);
    println!("  server:        {}:{}", args.server, args.udp_port);
    println!("  config:        {} pub → {} sub (fan-out={}), {}fps, {}B",
        args.publishers, r.num_subscribers, r.fan_out, args.fps, args.pkt_size);
    println!("  duration:      {:.1}s", r.duration_secs);
    println!();

    // ── Publisher ──
    println!("  ── Publisher Throughput ──");
    println!("  tx_packets:    {}", r.tx_packets);
    println!("  tx_bytes:      {} ({:.2} MB)",
        r.tx_bytes, r.tx_bytes as f64 / 1_048_576.0);
    println!("  tx_pps:        {:.1} pps", r.tx_pps);
    println!("  tx_throughput: {:.2} Mbps", r.tx_mbps);
    println!();

    if r.num_subscribers == 0 {
        println!("  ── No Subscribers ──");
        println!("  (use --subscribers N to add bench subscribers)");
        println!();
        return;
    }

    // ── Fan-out Aggregate ──
    println!("  ── Fan-out Aggregate ──");
    println!("  rx_total:      {} pkts ({:.2} MB)",
        r.rx_total_packets, r.rx_total_bytes as f64 / 1_048_576.0);
    println!("  rx_pps:        {:.1} pps ({}×{:.1} expected)",
        r.rx_total_pps, r.fan_out, r.tx_pps);
    println!("  rx_throughput: {:.2} Mbps", r.rx_total_mbps);
    println!("  lost:          {} ({:.3}%)", r.rx_total_lost, r.loss_rate);
    println!();

    // ── Latency ──
    println!("  ── End-to-End Latency ──");
    println!("  avg:           {:.0} µs ({:.2} ms)", r.latency_avg_us, r.latency_avg_us / 1000.0);
    println!("  p95:           {:.0} µs ({:.2} ms)", r.latency_p95_us, r.latency_p95_us / 1000.0);
    println!("  max:           {:.0} µs ({:.2} ms)", r.latency_max_us, r.latency_max_us / 1000.0);
    println!();

    // ── Per-Subscriber ──
    if r.sub_details.len() > 1 {
        println!("  ── Per-Subscriber Detail ──");
        for d in &r.sub_details {
            println!("  [{}] rx={} lost={} avg={:.0}µs p95={:.0}µs max={:.0}µs",
                d.id, d.rx_packets, d.rx_lost,
                d.latency_avg_us, d.latency_p95_us, d.latency_max_us);
        }
        println!();
    }
}

// ═══════════════════════════════════════════════════════════
// Conference report
// ═══════════════════════════════════════════════════════════

pub fn print_conference_report(args: &Args, r: &ConferenceResult) {
    let n = r.num_participants;
    let _expected_rx_per = (n - 1) as u64 * r.total_tx / n as u64;

    println!();
    println!("╔══════════════════════════════════════════════════════════╗");
    println!("║      LIGHT-SFU BENCHMARK REPORT (conference)            ║");
    println!("╚══════════════════════════════════════════════════════════╝");
    println!();
    println!("  label:         {}", args.label);
    println!("  server:        {}:{}", args.server, args.udp_port);
    println!("  participants:  {}", n);
    println!("  streams:       {} ({}×{})", r.total_streams, n, n - 1);
    println!("  config:        {}fps, {}B, {}s", args.fps, args.pkt_size, args.duration);
    println!("  duration:      {:.1}s", r.duration_secs);
    println!();

    // ── Input ──
    println!("  ── Input (all publishers) ──");
    println!("  total_tx:      {} pkts", r.total_tx);
    println!("  input_pps:     {:.1} pps ({}×{}fps expected)",
        r.input_pps, n, args.fps);
    println!("  input_bw:      {:.2} Mbps", r.input_mbps);
    println!();

    // ── Output ──
    let expected_total_rx = r.total_tx * (n - 1) as u64;
    println!("  ── Output (all subscribers) ──");
    println!("  expected_rx:   {} pkts (tx × {})", expected_total_rx, n - 1);
    println!("  actual_rx:     {} pkts", r.total_rx);
    println!("  output_pps:    {:.1} pps ({}×{}×{}fps expected)",
        r.output_pps, n, n - 1, args.fps);
    println!("  output_bw:     {:.2} Mbps", r.output_mbps);
    println!("  lost:          {} ({:.3}%)", r.total_lost, r.loss_rate);
    println!();

    // ── Latency ──
    println!("  ── End-to-End Latency ──");
    println!("  avg:           {:.0} µs ({:.2} ms)", r.latency_avg_us, r.latency_avg_us / 1000.0);
    println!("  p95:           {:.0} µs ({:.2} ms)", r.latency_p95_us, r.latency_p95_us / 1000.0);
    println!("  max:           {:.0} µs ({:.2} ms)", r.latency_max_us, r.latency_max_us / 1000.0);
    println!();

    // ── Per-Participant ──
    println!("  ── Per-Participant Detail ──");
    for p in &r.participants {
        let expected = (n - 1) as u64 * p.tx_packets;
        let status = if p.rx_lost == 0 { "✓" } else { "!" };
        println!("  [{}] tx={} rx={}/{} lost={} from={} avg={:.0}µs p95={:.0}µs max={:.0}µs {}",
            p.id, p.tx_packets, p.rx_packets, expected,
            p.rx_lost, p.rx_from_count,
            p.latency_avg_us, p.latency_p95_us, p.latency_max_us, status);
    }
    println!();
}
