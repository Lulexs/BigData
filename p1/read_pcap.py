from numpy import subtract
from scapy.all import rdpcap, Ether, IP, TCP, UDP, ICMP, ARP
import pandas as pd
import os
from datetime import datetime


PCAP_DIR = "iot_intrusion_dataset"
OUTPUT_CSV = "doc.csv"


def detect_protocol(packet):
    if packet.haslayer(TCP):
        return "TCP"
    if packet.haslayer(UDP):
        return "UDP"
    if packet.haslayer(ICMP):
        return "ICMP"
    if packet.haslayer(ARP):
        return "ARP"
    if packet.haslayer(IP):
        return "IP"
    return packet.lastlayer().name


def packet_to_row(packet, packet_no, file_name, category, subcategory):
    row = {
        "category": category,
        "sub_category": subcategory,
        "label": "benign" if category == "benign" else "attack",

        "packet_no": packet_no,
        "timestamp": float(packet.time),
        "datetime": datetime.fromtimestamp(float(packet.time)).isoformat(),

        "src_mac": None,
        "dst_mac": None,
        "src_ip": None,
        "dst_ip": None,
        "src_port": None,
        "dst_port": None,

        "protocol": detect_protocol(packet),
        "length": len(packet),
        "tcp_flags": None,
        "ttl": None,
        "window_size": None,
        "payload_len": 0,
    }

    if packet.haslayer(Ether):
        row["src_mac"] = packet[Ether].src
        row["dst_mac"] = packet[Ether].dst

    if packet.haslayer(IP):
        row["src_ip"] = packet[IP].src
        row["dst_ip"] = packet[IP].dst
        row["ttl"] = packet[IP].ttl

    if packet.haslayer(ARP):
        row["src_ip"] = packet[ARP].psrc
        row["dst_ip"] = packet[ARP].pdst

    if packet.haslayer(TCP):
        row["src_port"] = packet[TCP].sport
        row["dst_port"] = packet[TCP].dport
        row["tcp_flags"] = str(packet[TCP].flags)
        row["window_size"] = packet[TCP].window
        row["payload_len"] = len(packet[TCP].payload)

    elif packet.haslayer(UDP):
        row["src_port"] = packet[UDP].sport
        row["dst_port"] = packet[UDP].dport
        row["payload_len"] = len(packet[UDP].payload)

    return row


def main():
    rows = []

    for file_name in os.listdir(PCAP_DIR):
        if not file_name.endswith(".pcap"):
            continue

        splitted = file_name.split("-")
        category, subcategory = splitted[0], splitted[1]

        file_path = os.path.join(PCAP_DIR, file_name)

        packets = rdpcap(file_path)

        for packet_no, packet in enumerate(packets, start=1):
            rows.append(packet_to_row(packet, packet_no, file_name, category, subcategory))

        print(f"Finished processing file {file_name}")

    df = pd.DataFrame(rows)
    df.to_csv(OUTPUT_CSV, index=False)

    print(f"Saved {len(df)} packets to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()