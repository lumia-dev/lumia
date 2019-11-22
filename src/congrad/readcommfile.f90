program readcomm

    use netcdf

    integer :: nc_status, nc_id, var_id, n_state, dim_id
    real, dimension(:), allocatable :: init_g

    nc_status = nf90_open('comm_file.nc4', NF90_NOWRITE, nc_id)
    nc_status = nf90_inq_dimid(nc_id, 'n_state', dim_id)
    nc_status = nf90_inquire_dimension(nc_id, dim_id, len=n_state)
    allocate(init_g(n_state))
    init_g = 0.
    nc_status = nf90_inq_varid(nc_id, 'g_c', var_id)
    nc_status = nf90_get_var(nc_id, var_id, init_g, start=(/ 1, 1 /), count=(/ 1, n_state /))
    nc_status = nf90_close(nc_id)
    print*, sum(init_g), maxval(init_g), minval(init_g)
end program readcomm
