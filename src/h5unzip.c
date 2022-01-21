#include "hdf5.h"

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>

/*
 * TODO Let the user specify an output file name
 *
 * TODO Let the user choose the buffer size
 *
 * TODO Let the user specify a byte range they want to extract
 */

int main(int argc, char** argv)
{
  int retval = EXIT_SUCCESS;
  char fname[] = "h5unzip.bytes";
  char dname[] = "bytes";
  // 1 MiB read buffer
  const unsigned BUF_SIZE = 1024*1024;
  char buf[BUF_SIZE];

  assert(argc > 1);

  // open the HDF5 file
  hid_t hfile = H5Fopen(argv[1], H5F_ACC_RDONLY, H5P_DEFAULT);
  if (H5I_INVALID_HID == hfile) {
    goto fail_h5fopen;
  }

  // get the output file ready
  FILE* fp = fopen(fname, "w+b");
  if (NULL == fp) {
    retval = EXIT_FAILURE;
    goto cleanup_h5file;
  }

  // open the dataset
  hid_t dset = H5Dopen(hfile, dname, H5P_DEFAULT);
  if (H5I_INVALID_HID == dset) {
      retval = EXIT_FAILURE;
      goto cleanup_file;
  }

  hid_t fspace = H5Dget_space(dset);
  if (H5I_INVALID_HID == fspace) {
    retval = EXIT_FAILURE;
    goto cleanup_dset;
  }

  hssize_t total;
  hid_t mspace;
  if (0 > (total = H5Sget_simple_extent_npoints(fspace)) ||
      H5I_INVALID_HID == (mspace = H5Screate_simple(1,
                                                    (hsize_t[]){BUF_SIZE},
                                                    NULL))) {
    retval = EXIT_FAILURE;
    goto cleanup_fspace;
  }

  // main read loop
  for (hsize_t i = 0; i < (hsize_t)total; i += BUF_SIZE) {
    hsize_t start = i;
    hsize_t block = (i + BUF_SIZE < total) ? BUF_SIZE : total - i;

    // select & read & write
    if (0 > H5Sselect_hyperslab(fspace, H5S_SELECT_SET, &start, NULL,
                                (hsize_t[]){1}, (hsize_t[]){block}) ||
        0 > H5Sselect_hyperslab(mspace, H5S_SELECT_SET, (hsize_t[]){0}, NULL,
                                (hsize_t[]){1}, (hsize_t[]){block}) ||
        0 > H5Dread(dset, H5T_NATIVE_CHAR, mspace, fspace, H5P_DEFAULT, buf) ||
        block != (hsize_t)fwrite(buf, 1, (size_t)block, fp)) {
      retval = EXIT_FAILURE;
      break;
    }
  }

  H5Sclose(mspace);
cleanup_fspace:
  H5Sclose(fspace);
cleanup_dset:
  H5Dclose(dset);
cleanup_file:
  fclose(fp);
cleanup_h5file:
  H5Fclose(hfile);
fail_h5fopen:
  return retval;
}
