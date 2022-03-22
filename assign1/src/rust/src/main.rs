use std::time::Instant;

fn on_mult(n: usize) {
    let mut pha: Vec<f64> = vec![0.0; n * n];
    let mut phb: Vec<f64> = vec![0.0; n * n];
    let mut phc: Vec<f64> = vec![0.0; n * n];

    for i in 0..n {
        for j in 0..n {
            pha[i * n + j] = 1.0;
        }
    }

    for i in 0..n {
        for j in 0..n {
            phb[i * n + j] = (i + 1) as f64;
        }
    }

    let time = Instant::now();

    for i in 0..n {
        for j in 0..n {
            let mut temp = 0.0;
            for k in 0..n {
                temp += pha[i * n + k] * phb[k * n + j];
            }
            phc[i * n + j] = temp;
        }
    }

    println!(
        "Time: {} seconds",
        (time.elapsed().as_millis() as f64) / 1000.0
    );

    // display 10 elements of the result matrix to verify correctness
    println!("Result matrix:");
    for _i in 0..1 {
        for j in 0..std::cmp::min(10, n) {
            print!("{} ", phc[j]);
        }
    }
    println!("\n----");
}

fn on_mult_line(n: usize) {
    let mut pha: Vec<f64> = vec![0.0; n * n];
    let mut phb: Vec<f64> = vec![0.0; n * n];
    let mut phc: Vec<f64> = vec![0.0; n * n];

    for i in 0..n {
        for j in 0..n {
            pha[i * n + j] = 1.0;
        }
    }

    for i in 0..n {
        for j in 0..n {
            phb[i * n + j] = (i + 1) as f64;
        }
    }

    let time = Instant::now();

    for i in 0..n {
        for k in 0..n {
            for j in 0..n {
                phc[i * n + j] += pha[i * n + k] * phb[k * n + j];
            }
        }
    }

    println!(
        "Time: {} seconds",
        (time.elapsed().as_millis() as f64) / 1000.0
    );

    // display 10 elements of the result matrix to verify correctness
    println!("Result matrix:");
    for _i in 0..1 {
        for j in 0..std::cmp::min(10, n) {
            print!("{} ", phc[j]);
        }
    }
    println!("\n----");
}

fn on_mult_block(n: usize, block_size: usize) {
    let mut pha: Vec<f64> = vec![0.0; n * n];
    let mut phb: Vec<f64> = vec![0.0; n * n];
    let mut phc: Vec<f64> = vec![0.0; n * n];

    for i in 0..n {
        for j in 0..n {
            pha[i * n + j] = 1.0;
        }
    }

    for i in 0..n {
        for j in 0..n {
            phb[i * n + j] = (i + 1) as f64;
        }
    }

    let time = Instant::now();

    let n_blocks = n / block_size;
    for a_line_block in 0..n_blocks {
        for k_block in 0..n_blocks {
            for b_col_block in 0..n_blocks {
                for a_line in (a_line_block * block_size)..((a_line_block + 1) * block_size) {
                    for k in (k_block * block_size)..((k_block + 1) * block_size) {
                        for b_col in (b_col_block * block_size)..((b_col_block + 1) * block_size) {
                            phc[a_line * n + b_col] += pha[a_line * n + k] * phb[k * n + b_col];
                        }
                    }
                }
            }
        }
    }

   
    println!(
        "Time: {} seconds",
        (time.elapsed().as_millis() as f64) / 1000.0
    );

    // display 10 elements of the result matrix to verify correctness
    println!("Result matrix:");
    for _i in 0..1 {
        for j in 0..std::cmp::min(10, n) {
            print!("{} ", phc[j]);
        }
    }
    println!("\n----");
}

fn main() {
    on_mult(1024);
    on_mult_line(2048);
    on_mult_block(2048, 128);
}
