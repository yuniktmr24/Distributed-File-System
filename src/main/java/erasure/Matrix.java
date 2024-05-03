//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package erasure;

public class Matrix {
    private final int rows;
    private final int columns;
    private final byte[][] data;

    public Matrix(int initRows, int initColumns) {
        this.rows = initRows;
        this.columns = initColumns;
        this.data = new byte[this.rows][];

        for(int r = 0; r < this.rows; ++r) {
            this.data[r] = new byte[this.columns];
        }

    }

    public Matrix(byte[][] initData) {
        this.rows = initData.length;
        this.columns = initData[0].length;
        this.data = new byte[this.rows][];

        for(int r = 0; r < this.rows; ++r) {
            if (initData[r].length != this.columns) {
                throw new IllegalArgumentException("Not all rows have the same number of columns");
            }

            this.data[r] = new byte[this.columns];

            for(int c = 0; c < this.columns; ++c) {
                this.data[r][c] = initData[r][c];
            }
        }

    }

    public static Matrix identity(int size) {
        Matrix result = new Matrix(size, size);

        for(int i = 0; i < size; ++i) {
            result.set(i, i, (byte)1);
        }

        return result;
    }

    public String toString() {
        StringBuilder result = new StringBuilder();
        result.append('[');

        for(int r = 0; r < this.rows; ++r) {
            if (r != 0) {
                result.append(", ");
            }

            result.append('[');

            for(int c = 0; c < this.columns; ++c) {
                if (c != 0) {
                    result.append(", ");
                }

                result.append(this.data[r][c] & 255);
            }

            result.append(']');
        }

        result.append(']');
        return result.toString();
    }

    public String toBigString() {
        StringBuilder result = new StringBuilder();

        for(int r = 0; r < this.rows; ++r) {
            for(int c = 0; c < this.columns; ++c) {
                int value = this.get(r, c);
                if (value < 0) {
                    value += 256;
                }

                result.append(String.format("%02x ", value));
            }

            result.append("\n");
        }

        return result.toString();
    }

    public int getColumns() {
        return this.columns;
    }

    public int getRows() {
        return this.rows;
    }

    public byte get(int r, int c) {
        if (r >= 0 && this.rows > r) {
            if (c >= 0 && this.columns > c) {
                return this.data[r][c];
            } else {
                throw new IllegalArgumentException("Column index out of range: " + c);
            }
        } else {
            throw new IllegalArgumentException("Row index out of range: " + r);
        }
    }

    public void set(int r, int c, byte value) {
        if (r >= 0 && this.rows > r) {
            if (c >= 0 && this.columns > c) {
                this.data[r][c] = value;
            } else {
                throw new IllegalArgumentException("Column index out of range: " + c);
            }
        } else {
            throw new IllegalArgumentException("Row index out of range: " + r);
        }
    }

    public boolean equals(Object other) {
        if (!(other instanceof Matrix)) {
            return false;
        } else {
            for(int r = 0; r < this.rows; ++r) {
                if (!this.data[r].equals(((Matrix)other).data[r])) {
                    return false;
                }
            }

            return true;
        }
    }

    public Matrix times(Matrix right) {
        if (this.getColumns() != right.getRows()) {
            throw new IllegalArgumentException("Columns on left (" + this.getColumns() + ") " + "is different than rows on right (" + right.getRows() + ")");
        } else {
            Matrix result = new Matrix(this.getRows(), right.getColumns());

            for(int r = 0; r < this.getRows(); ++r) {
                for(int c = 0; c < right.getColumns(); ++c) {
                    byte value = 0;

                    for(int i = 0; i < this.getColumns(); ++i) {
                        value ^= Galois.multiply(this.get(r, i), right.get(i, c));
                    }

                    result.set(r, c, value);
                }
            }

            return result;
        }
    }

    public Matrix augment(Matrix right) {
        if (this.rows != right.rows) {
            throw new IllegalArgumentException("Matrices don't have the same number of rows");
        } else {
            Matrix result = new Matrix(this.rows, this.columns + right.columns);

            for(int r = 0; r < this.rows; ++r) {
                int c;
                for(c = 0; c < this.columns; ++c) {
                    result.data[r][c] = this.data[r][c];
                }

                for(c = 0; c < right.columns; ++c) {
                    result.data[r][this.columns + c] = right.data[r][c];
                }
            }

            return result;
        }
    }

    public Matrix submatrix(int rmin, int cmin, int rmax, int cmax) {
        Matrix result = new Matrix(rmax - rmin, cmax - cmin);

        for(int r = rmin; r < rmax; ++r) {
            for(int c = cmin; c < cmax; ++c) {
                result.data[r - rmin][c - cmin] = this.data[r][c];
            }
        }

        return result;
    }

    public byte[] getRow(int row) {
        byte[] result = new byte[this.columns];

        for(int c = 0; c < this.columns; ++c) {
            result[c] = this.get(row, c);
        }

        return result;
    }

    public void swapRows(int r1, int r2) {
        if (r1 >= 0 && this.rows > r1 && r2 >= 0 && this.rows > r2) {
            byte[] tmp = this.data[r1];
            this.data[r1] = this.data[r2];
            this.data[r2] = tmp;
        } else {
            throw new IllegalArgumentException("Row index out of range");
        }
    }

    public Matrix invert() {
        if (this.rows != this.columns) {
            throw new IllegalArgumentException("Only square matrices can be inverted");
        } else {
            Matrix work = this.augment(identity(this.rows));
            work.gaussianElimination();
            return work.submatrix(0, this.rows, this.columns, this.columns * 2);
        }
    }

    private void gaussianElimination() {
        byte[] var10000;
        int r;
        int rowBelow;
        int c;
        byte scale;
        for(r = 0; r < this.rows; ++r) {
            if (this.data[r][r] == 0) {
                for(rowBelow = r + 1; rowBelow < this.rows; ++rowBelow) {
                    if (this.data[rowBelow][r] != 0) {
                        this.swapRows(r, rowBelow);
                        break;
                    }
                }
            }

            if (this.data[r][r] == 0) {
                throw new IllegalArgumentException("Matrix is singular");
            }

            if (this.data[r][r] != 1) {
                scale = Galois.divide((byte) 1, this.data[r][r]);

                for(c = 0; c < this.columns; ++c) {
                    this.data[r][c] = Galois.multiply(this.data[r][c], scale);
                }
            }

            for(rowBelow = r + 1; rowBelow < this.rows; ++rowBelow) {
                if (this.data[rowBelow][r] != 0) {
                    scale = this.data[rowBelow][r];

                    for(c = 0; c < this.columns; ++c) {
                        var10000 = this.data[rowBelow];
                        var10000[c] ^= Galois.multiply(scale, this.data[r][c]);
                    }
                }
            }
        }

        for(r = 0; r < this.rows; ++r) {
            for(rowBelow = 0; rowBelow < r; ++rowBelow) {
                if (this.data[rowBelow][r] != 0) {
                    scale = this.data[rowBelow][r];

                    for(c = 0; c < this.columns; ++c) {
                        var10000 = this.data[rowBelow];
                        var10000[c] ^= Galois.multiply(scale, this.data[r][c]);
                    }
                }
            }
        }

    }
}
