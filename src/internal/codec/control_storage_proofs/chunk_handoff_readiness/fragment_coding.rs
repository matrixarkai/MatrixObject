/// Reed-Solomon coding over GF(2^8), for `k` data fragments and `m` redundancy
/// fragments of which any `k` of the `k + m` reconstruct the payload.
///
/// The sidecar scheme beside this one is a single XOR of every data fragment.
/// XOR is Reed-Solomon with `m = 1` and nothing more: it rebuilds exactly one
/// lost fragment and cannot rebuild two, whatever `k` is. A scheme that places
/// fragments on different nodes and survives losing several of them needs
/// `m > 1`, and that needs arithmetic in a field rather than a a single XOR.
///
/// Nothing here is wired into the store yet. It is the arithmetic, with the
/// property that matters -- any `k` of `k + m` -- checked exhaustively over
/// every loss pattern the configured ranges allow.

/// `x^8 + x^4 + x^3 + x^2 + 1`, the AES/CCSDS polynomial.
///
/// The generator is `0x02`, so walking the field is a left shift with a
/// conditional reduction rather than a multiply.
const GF_POLYNOMIAL: u16 = 0x11D;

/// `exp[i]` is `2^i` in the field, `log[v]` its discrete log.
///
/// The exponent table runs to 510 so a product's exponent needs no modulo:
/// `log(a) + log(b)` is at most 508.
struct GaloisTables {
    exp: [u8; 512],
    log: [u8; 256],
}

impl GaloisTables {
    const fn new() -> Self {
        let mut exp = [0u8; 512];
        let mut log = [0u8; 256];
        let mut value: u16 = 1;
        let mut index = 0usize;
        while index < 255 {
            exp[index] = value as u8;
            log[value as usize] = index as u8;
            value <<= 1;
            if value & 0x100 != 0 {
                value ^= GF_POLYNOMIAL;
            }
            index += 1;
        }
        // The tail repeats the cycle so an index past 254 needs no wrapping.
        let mut tail = 255usize;
        while tail < 512 {
            exp[tail] = exp[tail - 255];
            tail += 1;
        }
        Self { exp, log }
    }
}

static GALOIS: GaloisTables = GaloisTables::new();

fn gf_mul(a: u8, b: u8) -> u8 {
    if a == 0 || b == 0 {
        return 0;
    }
    GALOIS.exp[GALOIS.log[a as usize] as usize + GALOIS.log[b as usize] as usize]
}

fn gf_inverse(a: u8) -> u8 {
    debug_assert!(a != 0, "zero has no inverse");
    GALOIS.exp[255 - GALOIS.log[a as usize] as usize]
}

/// The `m x k` coefficients that produce the redundancy fragments.
///
/// Row `j`, column `i` is `1 / (x_j + y_i)` with `x_j = j` and
/// `y_i = m + i`, a **Cauchy** matrix. Cauchy is used rather than Vandermonde
/// because every square submatrix of a Cauchy matrix is invertible by
/// construction, which is exactly the "any `k` of `k + m`" property; a
/// Vandermonde matrix needs row reduction before that holds, and the reduction
/// is the step easiest to get subtly wrong.
///
/// `x_j` and `y_i` must be distinct, so `k + m` may not exceed 256.
fn redundancy_coefficients(k: usize, m: usize) -> Vec<Vec<u8>> {
    assert!(k > 0 && m > 0, "k and m must be positive");
    assert!(k + m <= 256, "k + m must fit the field: {k} + {m}");
    (0..m)
        .map(|j| {
            (0..k)
                .map(|i| {
                    let x = j as u8;
                    let y = (m + i) as u8;
                    gf_inverse(x ^ y)
                })
                .collect()
        })
        .collect()
}

/// The `m` redundancy fragments for `data`, each the length of a data fragment.
///
/// Every data fragment must already be the same length; the splitter pads.
pub(crate) fn rs_encode_redundancy(data: &[Vec<u8>], m: usize) -> Vec<Vec<u8>> {
    let k = data.len();
    let fragment_len = data.first().map_or(0, |fragment| fragment.len());
    assert!(
        data.iter().all(|fragment| fragment.len() == fragment_len),
        "data fragments must be equal length before encoding"
    );
    let coefficients = redundancy_coefficients(k, m);
    coefficients
        .iter()
        .map(|row| {
            let mut redundancy = vec![0u8; fragment_len];
            for (i, fragment) in data.iter().enumerate() {
                let coefficient = row[i];
                if coefficient == 0 {
                    continue;
                }
                for (slot, byte) in redundancy.iter_mut().zip(fragment.iter()) {
                    *slot ^= gf_mul(coefficient, *byte);
                }
            }
            redundancy
        })
        .collect()
}

/// Rebuild the `k` data fragments from any `k` surviving fragments.
///
/// `available` holds `(index, fragment)` pairs indexed over the whole set: `0..k`
/// are data fragments and `k..k + m` are redundancy fragments. Order does not
/// matter. More than `k` entries is accepted and the first `k` are used.
pub(crate) fn rs_reconstruct_data(
    available: &[(usize, Vec<u8>)],
    k: usize,
    m: usize,
) -> Result<Vec<Vec<u8>>, ObjectError> {
    if available.len() < k {
        return Err(ObjectError::Corruption(format!(
            "reconstruction needs {k} fragments and {} are present",
            available.len()
        )));
    }
    let fragment_len = available[0].1.len();
    if available.iter().any(|(_, fragment)| fragment.len() != fragment_len) {
        return Err(ObjectError::Corruption(
            "surviving fragments are not equal length".to_string(),
        ));
    }
    let coefficients = redundancy_coefficients(k, m);

    // One row of the generator matrix per surviving fragment: a data fragment's
    // row is a unit vector, a redundancy fragment's is its Cauchy row.
    let chosen = &available[..k];
    let mut matrix: Vec<Vec<u8>> = Vec::with_capacity(k);
    for (index, _) in chosen.iter() {
        if *index < k {
            let mut row = vec![0u8; k];
            row[*index] = 1;
            matrix.push(row);
        } else {
            let redundancy_index = index - k;
            if redundancy_index >= m {
                return Err(ObjectError::Corruption(format!(
                    "fragment index {index} is outside a {k}+{m} set"
                )));
            }
            matrix.push(coefficients[redundancy_index].clone());
        }
    }

    // Invert by Gaussian elimination, carrying the surviving fragments along as
    // the right-hand side so the result is the data fragments directly.
    let mut rhs: Vec<Vec<u8>> = chosen.iter().map(|(_, fragment)| fragment.clone()).collect();
    for column in 0..k {
        let pivot = (column..k)
            .find(|&row| matrix[row][column] != 0)
            .ok_or_else(|| {
                ObjectError::Corruption(
                    "surviving fragments do not determine the payload".to_string(),
                )
            })?;
        if pivot != column {
            matrix.swap(pivot, column);
            rhs.swap(pivot, column);
        }
        let inverse = gf_inverse(matrix[column][column]);
        for entry in matrix[column].iter_mut() {
            *entry = gf_mul(*entry, inverse);
        }
        for byte in rhs[column].iter_mut() {
            *byte = gf_mul(*byte, inverse);
        }
        for row in 0..k {
            if row == column {
                continue;
            }
            let factor = matrix[row][column];
            if factor == 0 {
                continue;
            }
            for index in 0..k {
                let shift = gf_mul(factor, matrix[column][index]);
                matrix[row][index] ^= shift;
            }
            for offset in 0..fragment_len {
                let shift = gf_mul(factor, rhs[column][offset]);
                rhs[row][offset] ^= shift;
            }
        }
    }
    Ok(rhs)
}
