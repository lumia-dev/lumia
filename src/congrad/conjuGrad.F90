module conjuGrad

  !==========================================================
  ! Conjugate gradient minimization
  ! (based on ECMWF Conjugate Gradient / Lanczos algorithm
  ! provided by Mike Fisher)
  !
  ! Reads in gradient from netcdf file, writes out next state vector to netcdf file
  ! Sourish Basu 01/2010
  !==========================================================

  use netcdf

  implicit none

  private

  public :: init_CONGRAD, simul_CONGRAD, CONGRAD, save_Eigenvectors, free_CONGRAD
  public :: n_state, congrad_finished, iter_max, n_iter, iter, io, interface_file_name
  public :: init_gradnorm_iter, write_intermediate_states, save_Trajectory
!   public :: init_x, init_g

  !===================================
  ! variables required for CONGRAD
  !===================================

  real                   :: preduc            ! required gradient norm reduction
!  real                   :: min_eigval        ! eigenvalue at which congrad should be stopped

  integer                :: i_Eigen           ! index for eigenvalue / vector
  integer                :: n_Eigen           ! number of converged eigenpairs

  real, allocatable      :: EigenVal(:)       ! converged eigenvalues
  real, allocatable      :: EigenVec(:,:)     ! converged eigenvectors
!   real, allocatable      :: init_x(:), init_g(:)    ! state and gradient vectors

  real                   :: grad_norm         ! norm of gradient
  real                   :: pkappa            ! required accuracy for eigenpairs

  character(len=200)     :: outdir            ! congrad output dir
  character(len=200)     :: outfname          ! congrad output file

  integer                :: congrad_finished  ! flag to indicate whether CONGRAD iteration is completed

  !============================================
  ! variables that were initially read in from global_data, var4d, dims
  ! and the rcFile, but should now be set within the subroutine
  !============================================

  integer                :: iter, iter_max, runid, kstatus, n_iter, io, iter_convergence
  real                   :: J_para, J_obs, J_tot
  integer                :: congrad_restart, n_state, init_gradnorm_iter
  logical                :: congrad_restore_ini
  integer,parameter      :: kdebug=9

  ! Sometimes it is useful to look at the trajectory of the solution. So we put in an option to
  ! write the intermediate states to a file
  logical               :: write_intermediate_states
  real, allocatable     :: xc_traject(:,:)

  character(len=256)     :: interface_file_name ! name of netcdf-3 file to communicate between fortran and python

contains

    subroutine error_handler(err_code)

        implicit none

        integer, intent(in) :: err_code
        write(0,'(a)') nf90_strerror(err_code)
        stop

    end subroutine error_handler

!    subroutine read_python(out_array, bsize)

!        !===============================================
!        ! Function for reading a list of floating point numbers from python
!        ! In all, 'total_size' numbers are read, in chunks of 'block_size'
!        !===============================================

!        implicit none

!        ! IO
!        real, intent(out)             :: out_array(:)
!        integer, intent(in), optional :: bsize

!        ! LOCAL VARIABLES
!        real, allocatable :: temp_array(:)
!        integer           :: remaining, position, total_size, block_size

!        ! START SUBROUTINE
!        if (.not. present(bsize)) then
!            block_size = 1000
!        else
!            block_size = bsize
!        end if
!        allocate(temp_array(block_size))
!        total_size = size(out_array)
!        remaining = total_size
!        position = 1

!        do while (remaining .ge. block_size)
!            read(*,*) temp_array(1:block_size)
!            out_array(position:position+block_size-1) = temp_array(:)
!            position = position + block_size
!            remaining = remaining - block_size
!        end do

!        if (remaining .gt. 0) then
!            read(*,*) out_array(position:total_size)
!        end if

!        deallocate(temp_array)

!    end subroutine read_python

!    subroutine write_python(in_array, bsize)

!        !===============================================
!        ! Function for writing a list of floating point numbers
!        ! to python in chunks of 'block_size'
!        !===============================================

!        implicit none

!        ! IO
!        real, intent(in)              :: in_array(:)
!        integer, intent(in), optional :: bsize

!        ! LOCAL VARIABLES
!        integer            :: remaining, position, total_size, block_size
!        character(len=200) :: string_block, string_rem, block_format, rem_format

!        ! START SUBROUTINE

!        if (.not. present(bsize)) then
!            block_size = 1000
!        else
!            block_size = bsize
!        end if
!        total_size = size(in_array)
!        write(string_block,'(i10)') block_size
!        write(string_rem, '(i10)') mod(total_size, block_size)
!        block_format = '('//trim(adjustl(string_block))//'F20.12)'
!        rem_format = '('//trim(adjustl(string_rem))//'F20.12)'
!        position = 1
!        remaining = total_size

!        do while (remaining .ge. block_size)
!            write(*, block_format) in_array(position:position+block_size-1)
!            position = position + block_size
!            remaining = remaining - block_size
!        end do

!        if (remaining .gt. 0) then
!            write(*, rem_format) in_array(position:total_size)
!        end if

!    end subroutine write_python

    subroutine init_CONGRAD(init_x, init_g)

        !===============================================
        ! initialization of CONGRAD variable
        !
        !===============================================

        use m_option_parser
        implicit none

        !__IO___________________________________________________________________
        real, intent(out), allocatable :: init_x(:), init_g(:)

        !__LOCAL_VARIABLES______________________________________________________

        character(len = 200)       :: header
        integer             :: num_args, nc_status, nc_id, var_id, dim_id, iargc
        character(len=1)    :: path_sep = '/'
        integer             :: path_len, i, io_status
        type(option_t), allocatable :: program_options(:)
        logical             :: in_cmd_line, print_help

        call set_parser_options(with_equal_sign = .true.)
        allocate(program_options(4))
        call set_option(program_options, "--state-file" ,   "",      "", "name of the state trajectory file")
        call set_option(program_options, "--init-gradnorm", "",       1, "the iteration from which to count the gradient norm reduction")
        call set_option(program_options, "--help",          "", .false., "print help to screen and quit")
        call set_option(program_options, "--write-traject", "", .false., "write out the optimization trajectory, i.e., the intermediate x values")

        call check_options(program_options, io_status)
        if (io_status /= 0) then
            write(0,*) "Something wrong with parsing the command line"
            stop 23
        end if
        call parse_options(program_options)

        call get_option_value(program_options, "--help", print_help)
        if (print_help) then
            call print_help_message(program_options, program_name    = "conjuGrad", &
                                                     program_version = "1.2", &
                                                     author          = "Sourish Basu", &
                                                     copyright_years = "2013")
            stop
        end if

        call get_option_value(program_options, "--state-file", interface_file_name, in_cmd_line)
        if (.not. in_cmd_line) then
            write(0,*) "The name of the state file MUST be specified with the --state-file flag"
            stop 24
        end if

        call get_option_value(program_options, "--init-gradnorm", init_gradnorm_iter)
        call get_option_value(program_options, "--write-traject", write_intermediate_states)

        deallocate(program_options)

        interface_file_name = trim(adjustl(interface_file_name))
        ! Need to extract the folder name to write the congrad_debug.out and ConGrad_iters.txt
        path_len = scan(interface_file_name, path_sep, back=.true.)
        outdir = trim(adjustl(interface_file_name(1:path_len)))

        io = 51
        open(unit = io, file=trim(outdir)//'congrad_debug.out', form = 'formatted', status='replace')

        !__START_SUBROUTINE______________________________________________________

        ! counter / flag for CONGRAD iterations in subsequent runs

        ! set CONGRAD parameters
        !  pkappa = 1.0E-1        ! required accuracy for eigenpairs
        pkappa = 1.0        ! required accuracy for eigenpairs


        ! set data that would otherwise be read from the rc files
        congrad_restart = -1
        congrad_finished = 0

        ! set data that would otherwise be set by another module
!        kstatus = 10

        ! read n_state, iter_max, iter_convergence and preduc

        nc_status = nf90_open(interface_file_name, NF90_NOWRITE, nc_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_inq_dimid(nc_id, 'n_state', dim_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_inquire_dimension(nc_id, dim_id, len=n_state)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        ! The termination condition can be specified by either the maximum number of iterations,
        ! or the gradient norm reduction, or the value of the lowest eigenvalue. Try to read
        ! all three from the netcdf file, assigning reasonable default values in case they are
        ! not there. Note that 'reasonable' means strict, as in for most cases we would actually
        ! look for convergence before the default values are reached.
        nc_status = nf90_inquire_attribute(nc_id, NF90_GLOBAL, 'iter_max')
        if (nc_status /= NF90_NOERR) then
            iter_max = 200
        else
            nc_status = nf90_get_att(nc_id, NF90_GLOBAL, 'iter_max', iter_max)
            if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        end if
        nc_status = nf90_inquire_attribute(nc_id, NF90_GLOBAL, 'iter_convergence')
        if (nc_status /= NF90_NOERR) then
            iter_convergence = 1000
        else
            nc_status = nf90_get_att(nc_id, NF90_GLOBAL, 'iter_convergence', iter_convergence)
            if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        end if
        nc_status = nf90_inquire_attribute(nc_id, NF90_GLOBAL, 'preduc')
        if (nc_status /= NF90_NOERR) then
            preduc = 1.0E-12
        else
            nc_status = nf90_get_att(nc_id, NF90_GLOBAL, 'preduc', preduc)
            if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        end if
!        nc_status = nf90_inquire_attribute(nc_id, NF90_GLOBAL, 'min_eigval')
!        if (nc_status /= NF90_NOERR) then
!            min_eigval = 1.01
!        else
!            nc_status = nf90_get_att(nc_id, NF90_GLOBAL, 'min_eigval', min_eigval)
!            if (nc_status /= NF90_NOERR) call error_handler(nc_status)
!        end if

        ! allocate state vectors
        allocate(init_x(n_state))
        allocate(init_g(n_state))

        ! Put some info to the screen
        write(io,"('=======================')")
        write(io,"('  init_CONGRAD')")
        write(io,"('=======================')")
        write(io,*) 'congrad.iter_max    = ',iter_max
        write(io,*) 'congrad.iter_conv   = ',iter_convergence
        write(io,*) 'congrad.preduc      = ',preduc
        write(io,*) 'congrad.restart     = ',congrad_restart
        write(io,*) 'congrad.finished    = ',congrad_finished

        congrad_restore_ini = .false.

        nc_status = nf90_inq_varid(nc_id, 'x_c', var_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_get_var(nc_id, var_id, init_x, start=(/ 1, 1 /), count=(/ 1, n_state /))
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_inq_varid(nc_id, 'g_c', var_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_get_var(nc_id, var_id, init_g, start=(/ 1, 1 /), count=(/ 1, n_state /))
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_close(nc_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
!        call read_python(init_x, 1000)
!        call read_python(init_g, 1000)

        ! allocate arrays
        allocate(EigenVal(iter_max))
        allocate(EigenVec(n_state,iter_max))

        write(header, '(a)') 'iter       J_para        J_obs        J_tot    grad_norm'

        flush(io)

    end subroutine init_CONGRAD

    subroutine free_CONGRAD

        !===============================================
        ! clean-up
        ! deallocate variables
        !===============================================

        implicit none

        !__IO___________________________________________________________________

        !__LOCAL_VARIABLES______________________________________________________

        !__START_SUBROUTINE______________________________________________________

        deallocate(EigenVal)
        deallocate(EigenVec)
        if (write_intermediate_states) deallocate(xc_traject)
        flush(io)
        close(io)

    end subroutine free_CONGRAD

    subroutine simul_CONGRAD(n,x_c1,g_c1,outp)

        !===============================================
        ! perform one forward and one adjoint run
        ! evaluate gradient of costfunction at x_c1
        ! g_c1 = dJ/dx_c(x_c1)
        !===============================================

        implicit none

        !__IO___________________________________________________________________

        integer, intent(in)           :: n             ! dimension of state vector, useless, because it's the same as n_state
        real, intent(inout)           :: x_c1(n_state) ! state vector (to be optimized)
        real, intent(out)             :: g_c1(n_state) ! gradient of f (i.e. df / dx(i))
        logical, intent(IN)           :: outp          ! flag to control writing to output file

        !__LOCAL_VARIABLES______________________________________________________

        integer :: status ! tells python that a vector is being fed
!         real               :: cost
        character(len=200) :: fname, string_n, op_format
        integer :: nc_id, nc_status, var_id, dim_x, dim_g, dim_id

        !__START_SUBROUTINE______________________________________________________

        write(string_n,'(i10)') n_state
        op_format = '('//trim(adjustl(string_n))//'F20.12)'

        ! if congrad_finished == 0, calculate gradient
        ! if congrad_finished == 1, max_iter reached, so write congrad_finished to file and get out
        ! if congrad_finished == 2, run final simulation
        ! if congrad_finished == 3, this routine is not called

        nc_status = nf90_open(interface_file_name, NF90_WRITE, nc_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
!        nc_status = nf90_del_att(nc_id, NF90_GLOBAL, 'congrad_finished')
!        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
!        nc_status = nf90_enddef(nc_id)
!        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_put_att(nc_id, NF90_GLOBAL, 'congrad_finished', congrad_finished)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        if (congrad_finished == 1) then
            nc_status = nf90_close(nc_id)
            if (nc_status /= NF90_NOERR) call error_handler(nc_status)
            return
        end if
        ! at the first call, iter=1
        nc_status = nf90_inq_dimid(nc_id, 'dim_x', dim_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_inquire_dimension(nc_id, dim_id, len=dim_x)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_inq_dimid(nc_id, 'dim_g', dim_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_inquire_dimension(nc_id, dim_id, len=dim_g)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        ! If iter >= dim_g, then we need to evaluate the gradient.
        ! So we write the state, then stop the program.
        ! If iter < dim_g, then we don't even need to write the state.
        ! Just read the correct gradient.
        ! If congrad_finished == 2, then we write the state, and stop the program.
        if ((iter .ge. dim_g) .or. (congrad_finished == 2)) then
            nc_status = nf90_inq_varid(nc_id, 'x_c', var_id)
            if (nc_status /= NF90_NOERR) call error_handler(nc_status)
            nc_status = nf90_put_var(nc_id, var_id, x_c1(1:n_state), start=(/ iter+1, 1 /), count=(/ 1, n_state /))
            if (nc_status /= NF90_NOERR) call error_handler(nc_status)
            nc_status = nf90_close(nc_id)
            if (nc_status /= NF90_NOERR) call error_handler(nc_status)
            if (congrad_finished /= 2) stop ! otherwise, need to write the eigenvalues and eigenvectors
        else
            nc_status = nf90_inq_varid(nc_id, 'g_c', var_id)
            if (nc_status /= NF90_NOERR) call error_handler(nc_status)
            nc_status = nf90_get_var(nc_id, var_id, g_c1, start=(/ iter+1, 1 /), count=(/ 1, n_state /))
            if (nc_status /= NF90_NOERR) call error_handler(nc_status)
            nc_status = nf90_close(nc_id)
            if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        end if

        grad_norm   = sqrt( dot_product( g_c1, g_c1 ) )

!        if ( outp ) then
!           call output_CONGRAD
!        end if

        !     iter = iter + 1
        flush(io)
        return

    if (congrad_finished .gt. 0) write(*, '(a,i1)') 'simul ended with congrad_finished = ', congrad_finished

    end subroutine simul_CONGRAD

    subroutine CONGRAD (kvadim,px,pgrad,kmaxit)

        !====================================================================
        ! ECMWF Conjugate Gradient / Lanczos algorithm
        ! provided by Mike Fisher (June 2003)
        !
        ! modified by S. Basu
        ! - to use outside of TM5 4DVAR
        ! S. Basu 08/2010
        !====================================================================

        implicit none


        !__IO___________________________________________________________________

        integer, intent(IN)           :: kvadim         ! dimension of control vector
        real, intent(inout)           :: px(kvadim)
        ! IN:  guess at the vector which minimizes the cost        OUT: solution
        real, intent(inout)           :: pgrad(kvadim)
        ! IN:  gradient of cost function at initial 'px'           OUT: gradient at solution point
        integer, intent(inout)        :: kmaxit
        ! IN:  maximum number of iterations                        OUT: number of iterations performed


        !__LOCAL_VARIABLES______________________________________________________

        logical       :: ldevecs        ! T => calculate eigenvalues and eigenvectors
        logical       :: ortho          ! T => orthonormalize Lanczos vectors
        integer       :: kngood         ! number of converged eigenpairs

!         integer       :: indic
        integer       :: jm
        integer       :: info
        integer       :: kulerr
        integer       :: j
        integer       :: jk

        real, allocatable :: zbeta(:), zdelta(:), zwork(:,:), zqg0(:), zx0(:), zgrad0(:)
        real, allocatable :: zw(:), zcglwk(:,:), zv(:,:), zsstwrk(:), zritz(:), zbnds(:), zwork_copy(:,:)
        real, allocatable :: ptheta(:) ! converged eigenvalues
        real, allocatable :: pvect(:,:) ! converged eigenvectors

        real          :: zreqrd, zgnorm, dla, zbndlm
!         real          :: zdummy

        character(len = 40)    :: fmt


        !__START_SUBROUTINE______________________________________________________

        ! initialize arrays
        allocate(zbeta(2:kmaxit+1))
        allocate(zdelta(kmaxit))
        allocate(zwork(kmaxit,4))
        allocate(zwork_copy(kmaxit,4))
        allocate(zqg0(kmaxit+1))
        allocate(zx0(kvadim))
        allocate(zgrad0(kvadim))
        allocate(zw(kvadim))
        allocate(zcglwk(kvadim,kmaxit+1))
        allocate(zv(kmaxit+1,kmaxit+1))
        allocate(zsstwrk(2*kmaxit))
        allocate(zritz(kmaxit+1))
        allocate(zbnds(kmaxit))
        allocate(ptheta(kmaxit))
        allocate(pvect(kvadim,kmaxit))

        zbeta(:)=0.0
        zdelta(:)=0.0
        zwork(:,:) = 0.0
        zwork_copy = 0.0
        zqg0(:)=0.0
        zx0(:)=0.0
        zgrad0(:)=0.0
        zw(:)=0.0
        zcglwk(:,:) = 0.0
        zv(:,:) = 0.0
        zsstwrk(:)=0.0
        zritz(:)=0.0
        zbnds(:)=0.0

        ldevecs = .true.
        ortho = .true.

        kulerr=0
        zreqrd = preduc

        !--- save initial control vector and gradient

        zx0(:)    = px(:)
        zgrad0(:) = pgrad(:)
        zgnorm = sqrt(dot_product (pgrad(:),pgrad(:)))

        !--- generate initial Lanczos vector

        zcglwk(:,1) = pgrad(:)
        zcglwk(:,1) = zcglwk(:,1) / zgnorm

        zqg0(1) = dot_product (zcglwk(:,1),zgrad0(:))

        !PBi
!        zwork(:,:) = 0.0

        iter   = 1

        if (congrad_finished ==2) return

        !if(iter <= kmaxit) then
        if (congrad_finished == 0) then

           !PBf
           !PB  iter   = 1

           Lanczos_loop : do

              !--- trial point
              zw(:) = zx0(:) + zcglwk(:,iter) ! x_0 + \epsilon d_k (\epsilon=1)

              !--- evaluate Hessian time Lanczos vector

              !PB  call simul(indic,kvadim,zw,zdummy,pgrad,iter)
              !PBi
              call simul_CONGRAD(kvadim,zw,pgrad,.false.) ! pgrad = J'(x_0 + \epsilon d_k)
              !PBf

    !          call save_CONGRAD_test

              pgrad(:) = pgrad(:) - zgrad0(:) ! pgrad = J'(x_0 + \epsilon d_k) - g_0 = J''d_k (since \epsilon=1)

              zdelta(iter) = dot_product (zcglwk(:,iter),pgrad(:)) ! zdelta = <d_k, J''d_k>

              !--- Calculate the new Lanczos vector (This is the Lanczos recurrence)

              pgrad(:) = pgrad(:)-zdelta(iter)*zcglwk(:,iter) !

              if (iter > 1) pgrad(:) = pgrad(:) - zbeta(iter)*zcglwk(:,iter-1)

              !--- orthonormalize gradient against previous gradients

              if (ortho) then
                 do jm=iter,1,-1
                    dla = dot_product(pgrad(:),zcglwk(:,jm))
                    pgrad(:) = pgrad(:) - dla*zcglwk(:,jm)
                 enddo
              end if

              zbeta(iter+1) = sqrt(dot_product(pgrad,pgrad))

              zcglwk(:,iter+1) = pgrad(:) / zbeta(iter+1)

              zqg0(iter+1) = dot_product (zcglwk(:,iter+1),zgrad0(:))

              !--- calculate the reduction in the gradient norm

              zwork(1:iter,1) =  zdelta(1:iter)
              zwork(2:iter,2) =  zbeta (2:iter)
              zwork(1:iter,3) = -zqg0  (1:iter)

    !          call save_CONGRAD_test

              call sptsv(iter,1,zwork(1,1),zwork(2,2),zwork(1,3),kmaxit,info)
              if (info /= 0) then
                 write (KULERR,*) 'Error in congrad: SPTSV/DPTSV returned ',info
                 stop 5 ! Why 5? Because 'stop info' does not work, stupid Fortran!
              end if

              pgrad(:) = zgrad0(:) + zbeta(iter+1)*zcglwk(:,iter+1)*zwork(iter,3)

              do j=1,iter
                 pgrad(:) = pgrad(:) - zcglwk(:,j)*zqg0(j)
              enddo

              grad_norm=sqrt(dot_product(pgrad,pgrad))

              ! In case we're supposed to count the gradient norm reduction from a certain step,
              ! set zgnorm to be the gradient norm at that step
              if (iter < init_gradnorm_iter) zgnorm = grad_norm

              preduc = grad_norm/zgnorm

!              ! SB: Calculate eigenvalues upto this point, and use the lowest one to check whether the
!              ! Lanczos loop should be terminated. Looks like zv and zsstwrk are not used in the Lanczos
!              ! loop, so we can overwrite them. On the other hand, zwork is used, so we have to use a
!              ! different array.
!                zwork_copy(1:iter  ,4) = zdelta(1:iter)
!                zwork_copy(1:iter-1,1) = zbeta (2:iter)
!                if (iter /= 1) then
!                    call SSTEQR ('I',iter,zwork_copy(1,4),zwork_copy(1,1),zv,kmaxit+1,zsstwrk,info)
!                else
!                    zv(1,1) = 1.0
!                end if
!                zritz(1:iter) = zwork_copy(1:iter,4)
!                zbndlm = pkappa
!                zbnds(1:iter) = abs(zbeta(iter+1)*zv(iter,1:iter))/zritz(1:iter)
!                kngood=0
!                do jm=iter,1,-1
!                    if (zbnds(jm) <= zbndlm) then
!                        kngood=kngood+1
!                        ptheta(kngood) = zritz(jm)
!                    end if
!                end do
!              ! SB: End calculation

!              write (io,'(a,i3,a,e14.7,a,e14.7,a,e14.7,a,e14.7)') 'After iter ', iter, &
!                                                          ': ||p_grad(k)|| = ', grad_norm, &
!                                                          '    ||p_grad(0)|| = ', zgnorm, &
!                                                          '    grad norm reduction = ', 1.0/preduc, &
!                                                          '    lowest eigenvalue = ', ptheta(kngood)
              write (io,'(a,i3,a,es14.7,a,es14.7,a,es14.7)') 'After iter ', iter, &
                                                          ': ||p_grad(k)|| = ', grad_norm, &
                                                          '  ||p_grad(0)|| = ', zgnorm, &
                                                          '  grad norm reduction = ', 1.0/preduc
              flush(io)

              if (iter >= kmaxit) congrad_finished = 1
              if ((preduc <= zreqrd) .or. (iter >= iter_convergence)) congrad_finished = 2

              if (congrad_finished > 0) exit Lanczos_loop

              iter = iter+1

           enddo Lanczos_loop

           !PBi
        else
    !       iter = kmaxit
          iter = iter-1
        end if
        !PBf


        if (ldevecs) then

           !--- determine eigenvalues and eigenvectors of the tri-diagonal problem

           zwork(1:iter  ,4) = zdelta(1:iter)
           zwork(1:iter-1,1) = zbeta (2:iter)

           if (iter /= 1) then
              call SSTEQR ('I',iter,zwork(1,4),zwork(1,1),zv,kmaxit+1,zsstwrk,info)
           else
              zv(1,1) = 1.0
           endif

           zritz(1:iter) = zwork(1:iter,4)

           zbndlm = pkappa
           zbnds(1:iter) = abs(zbeta(iter+1)*zv(iter,1:iter))/zritz(1:iter)

           kngood=0
           do jm=iter,1,-1
              if (zbnds(jm) <= zbndlm) then
                 kngood=kngood+1
                 ptheta(kngood) = zritz(jm)

                 pvect(:,kngood) = 0.0
                 do jk=1,iter
                    pvect(:,kngood) =  pvect(:,kngood) + zcglwk(:,jk)*zv(jk,jm)
                 enddo

                 do jk=1,kngood-1
                    dla = dot_product (pvect(:,jk),pvect(:,kngood))
                    pvect(:,kngood) = pvect(:,kngood) - dla*pvect(:,jk)
                 enddo

                 dla = dot_product (pvect(:,kngood),pvect(:,kngood))
                 pvect(:,kngood) = pvect(:,kngood) / sqrt(dla)
              end if
           enddo


           !PBi
           n_Eigen = kngood         ! number of converged eigenpairs
           EigenVal(1:n_Eigen) = ptheta(1:n_Eigen)
           EigenVec(:,1:n_Eigen) = pvect(:,1:n_Eigen)

           write(io,*) 'number of converged eigenvalues=', n_Eigen

           do i_Eigen=1, n_Eigen
              write(io,*) i_Eigen, EigenVal(i_Eigen)
           enddo

        end if

        !--- calculate the solution

        print*, 'toto'
        print*, write_intermediate_states
        if (write_intermediate_states) allocate(xc_traject(iter+1,kvadim))
        px(:) = zx0(:)
        if (write_intermediate_states) xc_traject(1,:) = px(:)
        do j=1,iter
           px(:)    = px(:) + zcglwk(:,j)*zwork(j,3)
           if (write_intermediate_states) xc_traject(j+1,:) = px(:)
        end do

        kmaxit = iter

        ! deallocate arrays previously allocated
        deallocate(zbeta)
        deallocate(zdelta)
        deallocate(zwork)
        deallocate(zwork_copy)
        deallocate(zqg0)
        deallocate(zx0)
        deallocate(zgrad0)
        deallocate(zw)
        deallocate(zcglwk)
        deallocate(zv)
        deallocate(zsstwrk)
        deallocate(zritz)
        deallocate(zbnds)
        deallocate(ptheta)
        deallocate(pvect)

        return

    end subroutine CONGRAD

    subroutine save_Trajectory

        ! Local variables
        integer     :: nc_id, var_id, nc_status, dim_id_sta, dim_id_iter

        ! Start subroutine
        nc_status = nf90_open(interface_file_name, NF90_WRITE, nc_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_redef(nc_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_def_dim(nc_id, 'n_iter', size(xc_traject, 1), dim_id_iter)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_inq_dimid(nc_id, 'n_state', dim_id_sta)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_def_var(nc_id, 'xc_traject', NF90_DOUBLE, (/ dim_id_iter, dim_id_sta /), var_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_enddef(nc_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_put_var(nc_id, var_id, xc_traject(:,:))
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_close(nc_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)

    end subroutine save_Trajectory

    subroutine save_Eigenvectors

        !__LOCAL_VARIABLES______________________________________________________
        character(len=40) :: num_string, op_format
        integer           :: nc_id, var_id_eval, var_id_evec, nc_status, dim_id_eig, dim_id_sta
        real, allocatable :: EigenVec_transp(:,:)

        !__START_SUBROUTINE______________________________________________________

        nc_status = nf90_open(interface_file_name, NF90_WRITE, nc_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_redef(nc_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_def_dim(nc_id, 'n_eigen', n_Eigen, dim_id_eig)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_inq_dimid(nc_id, 'n_state', dim_id_sta)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_def_var(nc_id, 'eigenvalues', NF90_DOUBLE, (/ dim_id_eig /), var_id_eval)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_def_var(nc_id, 'eigenvectors', NF90_DOUBLE, (/ dim_id_eig, dim_id_sta /), var_id_evec)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_enddef(nc_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_put_var(nc_id, var_id_eval, EigenVal(1:n_Eigen))
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        ! calling nf90_put_var with transpose(EigenVec) puts too big an array on the stack, so
        ! compute the transpose before trying to save it
        allocate(EigenVec_transp(n_Eigen, n_state))
        EigenVec_transp = transpose(EigenVec(:,1:n_Eigen))
        nc_status = nf90_put_var(nc_id, var_id_evec, EigenVec_transp)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        deallocate(EigenVec_transp)
        nc_status = nf90_put_att(nc_id, NF90_GLOBAL, 'last_iter', iter)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)
        nc_status = nf90_close(nc_id)
        if (nc_status /= NF90_NOERR) call error_handler(nc_status)

    end subroutine save_Eigenvectors

end module conjuGrad
