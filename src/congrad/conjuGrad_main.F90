program main

use conjuGrad, only : init_CONGRAD, simul_CONGRAD, CONGRAD, save_Eigenvectors, free_CONGRAD ! the subroutines
use conjuGrad, only : n_state, congrad_finished, iter_max, n_iter, iter, io, interface_file_name ! the variables
use conjuGrad, only : write_intermediate_states, save_Trajectory
! use conjuGrad, only : init_x, init_g

implicit none

real, allocatable  :: x_c(:), g_c(:)

! initialize CONGRAD
call init_CONGRAD(x_c, g_c)

! allocate(x_c(n_state))
! allocate(g_c(n_state))

! x_c(:) = init_x(:)
! g_c(:) = init_g(:)

! deallocate(init_x)
! deallocate(init_g)

!call simul_CONGRAD(n_state, x_c, g_c, .true.)
!call save_CONGRAD_0(x_c, g_c)

if(congrad_finished <=1) call CONGRAD( n_state, x_c, g_c, iter_max)

if (congrad_finished == 1) then
    ! max_iter has been reached without convergence
    n_iter = iter
    call simul_CONGRAD(n_state, x_c, g_c, .false.)
else if (congrad_finished == 2) then
    ! convergence has been reached
    ! store total number of iterations performed
    n_iter = iter
    ! final calculation of x,g
    iter = iter + 1
    call simul_CONGRAD(n_state, x_c, g_c, .true.)
    call save_Eigenvectors
    print*, write_intermediate_states
    if (write_intermediate_states) call save_Trajectory
    write(io,*) '============================================='
    write(io,*) ' CONGRAD minimization completed successfully '
    write(io,*) '============================================='
end if

if (congrad_finished /= 2) write(io,*) 'main: a posteriori solution not yet calculated'
!==========================================
! a posteriori covariance matrix
! in this version we only save the eigen(vectors/values) and leave the rest to python
!==========================================

! free vectors allocated for CONGRAD algorithm
call free_CONGRAD

end program main
