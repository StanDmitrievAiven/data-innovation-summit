export interface RegionDef {
  region: string;
  countries: string[];
  weight: number;
}

// EMEA-heavy weighting so the "EMEA, last 30 days" demo question is meaningful.
export const REGIONS: RegionDef[] = [
  { region: "EMEA-DACH",    countries: ["DE", "AT", "CH"],       weight: 18 },
  { region: "EMEA-FR",      countries: ["FR", "BE", "LU"],       weight: 14 },
  { region: "EMEA-UK",      countries: ["GB", "IE"],             weight: 14 },
  { region: "EMEA-NORDICS", countries: ["SE", "NO", "DK", "FI"], weight: 12 },
  { region: "EMEA-IT",      countries: ["IT"],                   weight:  9 },
  { region: "EMEA-IBERIA",  countries: ["ES", "PT"],             weight:  8 },
  { region: "AMER",         countries: ["US", "CA", "MX", "BR"], weight: 15 },
  { region: "APAC",         countries: ["JP", "AU", "SG", "IN"], weight: 10 },
];

export interface ProductSeed {
  sku: string;
  name: string;
  category: string;
  price: number;
  inventory: number;
}

export const PRODUCT_CATALOG: ProductSeed[] = [
  // Electronics
  { sku: "ELC-WEB-001", name: "Wireless Earbuds Pro",        category: "Electronics", price:  99.00, inventory: 500 },
  { sku: "ELC-WAT-002", name: "Smart Watch Series 5",        category: "Electronics", price: 249.00, inventory: 220 },
  { sku: "ELC-SPK-003", name: "Bluetooth Speaker Mini",      category: "Electronics", price:  59.00, inventory: 400 },
  { sku: "ELC-HUB-004", name: "USB-C 7-in-1 Hub",            category: "Electronics", price:  39.00, inventory: 600 },
  { sku: "ELC-STN-005", name: "Aluminium Phone Stand",       category: "Electronics", price:  19.00, inventory: 800 },
  { sku: "ELC-KBD-006", name: "Mechanical Keyboard 75%",     category: "Electronics", price: 129.00, inventory: 180 },
  { sku: "ELC-MSE-007", name: "Wireless Ergo Mouse",         category: "Electronics", price:  49.00, inventory: 350 },
  { sku: "ELC-CAM-008", name: "HD Webcam 1080p",             category: "Electronics", price:  79.00, inventory: 240 },
  { sku: "ELC-CHR-009", name: "65W GaN Charger",             category: "Electronics", price:  35.00, inventory: 700 },
  { sku: "ELC-MON-010", name: "27\" 4K Monitor",             category: "Electronics", price: 379.00, inventory:  90 },
  { sku: "ELC-LMP-011", name: "Adjustable Desk Lamp",        category: "Electronics", price:  45.00, inventory: 320 },
  { sku: "ELC-PWR-012", name: "20000mAh Power Bank",         category: "Electronics", price:  39.00, inventory: 450 },
  // Apparel
  { sku: "APP-TEE-101", name: "Classic Cotton T-Shirt",      category: "Apparel",     price:  19.00, inventory: 1200 },
  { sku: "APP-HOO-102", name: "Heavyweight Hoodie",          category: "Apparel",     price:  59.00, inventory:  600 },
  { sku: "APP-SNK-103", name: "Everyday Sneakers",           category: "Apparel",     price:  89.00, inventory:  300 },
  { sku: "APP-CAP-104", name: "Snapback Cap",                category: "Apparel",     price:  24.00, inventory:  900 },
  { sku: "APP-BAG-105", name: "Daypack Backpack 20L",        category: "Apparel",     price:  69.00, inventory:  280 },
  { sku: "APP-JKT-106", name: "Lightweight Rain Jacket",     category: "Apparel",     price:  99.00, inventory:  220 },
  { sku: "APP-JNS-107", name: "Slim Fit Jeans",              category: "Apparel",     price:  69.00, inventory:  500 },
  { sku: "APP-SOX-108", name: "Merino Hiking Socks 3-Pack",  category: "Apparel",     price:  29.00, inventory: 1500 },
  { sku: "APP-SCF-109", name: "Wool Scarf",                  category: "Apparel",     price:  39.00, inventory:  400 },
  { sku: "APP-GLO-110", name: "Touchscreen Gloves",          category: "Apparel",     price:  25.00, inventory:  650 },
  // Home
  { sku: "HOM-COF-201", name: "Drip Coffee Maker",           category: "Home",        price: 109.00, inventory: 180 },
  { sku: "HOM-AIR-202", name: "HEPA Air Purifier",           category: "Home",        price: 199.00, inventory: 120 },
  { sku: "HOM-LED-203", name: "Smart LED Bulb 4-Pack",       category: "Home",        price:  29.00, inventory: 700 },
  { sku: "HOM-SCL-204", name: "Digital Kitchen Scale",       category: "Home",        price:  19.00, inventory: 450 },
  { sku: "HOM-YOG-205", name: "Cork Yoga Mat",               category: "Home",        price:  55.00, inventory: 260 },
  { sku: "HOM-KET-206", name: "Stainless Electric Kettle",   category: "Home",        price:  49.00, inventory: 300 },
  { sku: "HOM-BLD-207", name: "High-Speed Blender",          category: "Home",        price: 159.00, inventory: 130 },
  { sku: "HOM-TWL-208", name: "Bath Towel Set (4)",          category: "Home",        price:  45.00, inventory: 380 },
  { sku: "HOM-PIL-209", name: "Memory Foam Pillow",          category: "Home",        price:  59.00, inventory: 240 },
  { sku: "HOM-CDL-210", name: "Soy Candle Trio",             category: "Home",        price:  35.00, inventory: 520 },
  // Books / Media
  { sku: "BKS-COK-301", name: "Mediterranean Cookbook",      category: "Books",       price:  29.00, inventory: 400 },
  { sku: "BKS-TRV-302", name: "European Travel Guide 2026",  category: "Books",       price:  24.00, inventory: 350 },
  { sku: "BKS-SCI-303", name: "Pocket Astronomy Atlas",      category: "Books",       price:  19.00, inventory: 270 },
  { sku: "BKS-FIC-304", name: "Best of European Fiction",    category: "Books",       price:  22.00, inventory: 480 },
  { sku: "BKS-BIZ-305", name: "Modern Data Platforms",       category: "Books",       price:  39.00, inventory: 200 },
  // Outdoors / Sports
  { sku: "SPT-BTL-401", name: "Insulated Water Bottle 1L",   category: "Sports",      price:  29.00, inventory: 800 },
  { sku: "SPT-BIK-402", name: "Bike Repair Kit",             category: "Sports",      price:  35.00, inventory: 220 },
  { sku: "SPT-RUN-403", name: "Reflective Running Belt",     category: "Sports",      price:  19.00, inventory: 540 },
  { sku: "SPT-CAM-404", name: "Compact Camping Stove",       category: "Sports",      price:  49.00, inventory: 180 },
  { sku: "SPT-HEL-405", name: "Cycling Helmet",              category: "Sports",      price:  79.00, inventory: 200 },
  // Beauty
  { sku: "BTY-SKN-501", name: "Daily Moisturiser",           category: "Beauty",      price:  29.00, inventory: 700 },
  { sku: "BTY-FRG-502", name: "Citrus Eau de Toilette",      category: "Beauty",      price:  79.00, inventory: 260 },
  { sku: "BTY-MAK-503", name: "Mineral Lipstick",            category: "Beauty",      price:  19.00, inventory: 900 },
  { sku: "BTY-HAR-504", name: "Argan Hair Oil",              category: "Beauty",      price:  25.00, inventory: 500 },
  { sku: "BTY-BTH-505", name: "Lavender Bath Salts",         category: "Beauty",      price:  18.00, inventory: 650 },
  // Office
  { sku: "OFF-CHR-601", name: "Ergonomic Office Chair",      category: "Office",      price: 299.00, inventory:  90 },
  { sku: "OFF-DSK-602", name: "Standing Desk Converter",     category: "Office",      price: 219.00, inventory: 120 },
  { sku: "OFF-NTB-603", name: "Premium A5 Notebook",         category: "Office",      price:  19.00, inventory: 800 },
  { sku: "OFF-PEN-604", name: "Rollerball Pen Set",          category: "Office",      price:  39.00, inventory: 480 },
];

const FIRST_NAMES = [
  "Alex", "Maria", "Liam", "Sofia", "Noah", "Emma", "Lucas", "Mia",
  "Mateo", "Olivia", "Ethan", "Ava", "Leo", "Isabella", "Daniel", "Aria",
  "Hugo", "Chloe", "Jules", "Léa", "Jonas", "Lina", "Henri", "Nora",
  "Erik", "Astrid", "Oliver", "Elin", "Matteo", "Giulia", "Carlos", "Lucía",
  "Yuki", "Hiro", "Aarav", "Priya", "Tom", "Ella", "Ben", "Lily",
];

const LAST_NAMES = [
  "Schmidt", "Müller", "Becker", "Dubois", "Martin", "Bernard", "Smith",
  "Jones", "Brown", "Taylor", "Andersson", "Berg", "Olsen", "Hansen",
  "Rossi", "Bianchi", "García", "Martínez", "Sánchez", "Silva", "Costa",
  "Tanaka", "Sato", "Patel", "Singh", "Kim", "Wang", "Lee",
];

export function pickWeighted<T extends { weight: number }>(items: T[]): T {
  const total = items.reduce((s, x) => s + x.weight, 0);
  let r = Math.random() * total;
  for (const item of items) {
    r -= item.weight;
    if (r <= 0) return item;
  }
  return items[items.length - 1];
}

export function pickRandom<T>(items: T[]): T {
  return items[Math.floor(Math.random() * items.length)];
}

export function randomBetween(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

export function randomCustomer(): {
  email: string;
  name: string;
  region: string;
  country: string;
} {
  const region = pickWeighted(REGIONS);
  const country = pickRandom(region.countries);
  const first = pickRandom(FIRST_NAMES);
  const last = pickRandom(LAST_NAMES);
  // Add a short random suffix to keep emails unique even for duplicate names.
  const suffix = Math.random().toString(36).slice(2, 8);
  const email = `${first}.${last}.${suffix}@example.com`.toLowerCase();
  const name = `${first} ${last}`;
  return { email, name, region: region.region, country };
}
