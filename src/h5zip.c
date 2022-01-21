#include "hdf5.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

/*
 * TODO Let the user specify an output file name
 *
 * TODO Add an attribute that captures the path name provided
 *      or a user-defined identifier
 *
 * TODO Add an attribute for the string encoding (if provided)
 *
 * TODO Let the user choose a buffer/chunk size
 */

int main(int argc, char** argv)
{
  int retval = EXIT_SUCCESS;
  char fname[] = "h5zip.h5";
  char dname[] = "bytes";
  // 1 MiB read buffer
  const unsigned BUF_SIZE = 1024*1024;
  char buf[BUF_SIZE];

  assert(argc > 1);

  // get the input file ready
  FILE* fp = fopen(argv[1], "rb");
  if (NULL == fp) {
    retval = EXIT_FAILURE;
    goto fail_open;
  }

  // create the HDF5 file
  hid_t hfile = H5Fcreate(fname, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);
  if (H5I_INVALID_HID == hfile) {
    goto cleanup_file;
  }

  // create the dataset
  hid_t dset = H5I_INVALID_HID;

  {
    hid_t fspace = H5Screate_simple(1, (hsize_t[]){0},
                                    (hsize_t[]){H5S_UNLIMITED});
    if (H5I_INVALID_HID == fspace) {
      retval = EXIT_FAILURE;
      goto fail_dset;
    }
    hid_t dcpl = H5Pcreate(H5P_DATASET_CREATE);
    if (H5I_INVALID_HID == dcpl) {
      retval = EXIT_FAILURE;
      goto fail_dset;
    }
    if (H5Pset_chunk(dcpl, 1, (hsize_t[]){BUF_SIZE}) < 0 ||
        H5Pset_deflate(dcpl, 6) < 0) {
      retval = EXIT_FAILURE;
      goto fail_dset;
    }
    if ((dset = H5Dcreate(hfile, dname, H5T_NATIVE_CHAR, fspace,
                          H5P_DEFAULT, dcpl, H5P_DEFAULT)) == H5I_INVALID_HID) {
      retval = EXIT_FAILURE;
      goto fail_dset;
    }

  fail_dset:
    if (H5I_INVALID_HID != dcpl)
      H5Pclose(dcpl);
    if (H5I_INVALID_HID != fspace)
      H5Sclose(fspace);
  }

  if (H5I_INVALID_HID == dset) {
    retval = EXIT_FAILURE;
    goto cleanup_h5file;
  }

  // in-memory dataspace is an invariant of the read loop
  hid_t mspace = H5Screate_simple(1, (hsize_t[]){BUF_SIZE}, NULL);
  if (H5I_INVALID_HID == mspace) {
    retval = EXIT_FAILURE;
    goto cleanup_h5file;
  }

  // main read loop
  size_t block = 0, total = 0;
  hid_t fspace;
  hsize_t start;
  while (0 != (block = fread(buf, 1, BUF_SIZE, fp))) {
    start = (hsize_t) total;  // start is total before the update
    total += block;           // new total
    if (0 > H5Dset_extent(dset, (hsize_t[]){total})) {
        retval = EXIT_FAILURE;
        break;
    }
    fspace = H5Dget_space(dset);
    if (H5I_INVALID_HID == fspace) {
      retval = EXIT_FAILURE;
      break;
    }

    // select & write & close file dataspace
    if (0 > H5Sselect_hyperslab(fspace, H5S_SELECT_SET, &start, NULL,
                                (hsize_t[]){1}, (hsize_t[]){block}) ||
        0 > H5Sselect_hyperslab(mspace, H5S_SELECT_SET, (hsize_t[]){0}, NULL,
                                (hsize_t[]){1}, (hsize_t[]){block}) ||
        0 > H5Dwrite(dset, H5T_NATIVE_CHAR, mspace, fspace, H5P_DEFAULT, buf) ||
        0 > H5Sclose(fspace)) {
      retval = EXIT_FAILURE;
      if (H5I_INVALID_HID != fspace)
        H5Sclose(fspace);  // the only resource acquired in the WHILE block
      break;
    }
  }

  H5Sclose(mspace);
  H5Dclose(dset);

cleanup_h5file:
  H5Fclose(hfile);
cleanup_file:
  fclose(fp);
fail_open:
  return retval;
}
