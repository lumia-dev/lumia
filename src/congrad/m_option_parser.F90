module m_option_parser
  !!****h* src/utils/m_option_parser
  !!  NAME
  !!    m_option_parser  --  command line option parser module
  !!  SYNOPSIS
  !!    use m_option_parser
  !!  DESCRIPTION
  !!    This module makes some commands available to set command line options
  !!    for a program and to parse the supplied ones at run-time. In addition,
  !!    the valid options are filtered. And the results can be returned.
  !!    There are five supported variables: real, complex, integer, character
  !!    and logical. Of the first two, single and double precision are
  !!    supported for the GNU compilers (gfortran and g95) and for the intel
  !!    compiler, quadruple precision is also supported.
  !!
  !!    This module can be compiled as follows:
  !!      $ $COMPILER_NAME -c -o m_option_parser m_option_parser.f90
  !!    Then, the test program can be compiled:
  !!      $ $COMPILER_NAME -o test_option_parser test_option_parser.f90 \
  !!      m_option_parser.o
  !!    Where $COMPILER_NAME must be substituted by the actual name of the
  !!    compiler, e.g. 'ifort' (intel fortran compiler) or 'gfortran' (gnu
  !!    fortran compiler).
  !!  MODIFICATION HISTORY
  !!    2007-10-04 -- WvH : Initial version
  !!    2007-10-05 -- WvH : Extended the set_options interface to real, integer,
  !!                        logical and character command line arguments.
  !!    2007-10-06 -- WvH : Command line is parsed (parse_options), now only need
  !!                        to filter out the valid arguments.
  !!    2007-10-08 -- WvH : - :%s/valid_options/program_options/g
  !!                        - There is still a bug in the parser when invalid
  !!                          options are encountered in combination with keys
  !!                          that are defined for more than 1 option.
  !!                        - All subroutines now have headers.
  !!    2007-10-09 -- WvH : All features implemented.
  !!    2007-10-13 -- WvH : Nope, I missed double precision real input, works now.
  !!    2007-10-14 -- WvH : gfortran complained about key_t being private but
  !!                        option_t not being private. Made all members of
  !!                        option_t private outside this module.
  !!    2007-12-27 -- WvH : Options can be defined by a long key only, with the
  !!                        short key equal to an empty string ("").
  !!                        Every option still needs a long key.
  !!                        set_option subroutine now has an optional 6th
  !!                        argument: group_name, which makes it possible to show
  !!                        the options per categories when print_help_message is
  !!                        called.
  !!    2007-12-28 -- WvH : Complex numbers work, they must be defined on the
  !!                        command line as in the following:
  !!                        ./test -p "(1.,-2.)"
  !!    2007-12-29 -- WvH : API change, 'set_option' now accepts the whole
  !!                        allocated derived type instead of 1 of them, just
  !!                        like get_option_value.
  !!    2007-12-30 -- WvH : Raise error if number of options set is not equal to
  !!                        the size of the option_t derived type
  !!    2008-01-10 -- WvH : API change, 'get_option_value' has a 4th logical
  !!                        (optional) argument, that is true if the argument was
  !!                        supplied on the command line, and false if it was not.
  !!    2008-01-25 -- WvH : Implemented --option=value and -o=value syntax (as in
  !!                        flightgear), needs more testing though.
  !!    2008-02-27 -- WvH : Added quadruple precision real and complex options.
  !!    2008-03-02 -- WvH : GNU fortran compilers (gfortran and g95) do not define
  !!                        quadruple precision variables. Added preprocessor
  !!                        directives to get around this
  !!    2008-08-08 -- WvH : Length of long keys and character option values can
  !!                        be set inside the module, but not using a call to
  !!                        set_parser_options, as parametric derived types
  !!                        are not yet implemented.
  !!    2008-08-31 -- WvH : Main loop in parse_options rewritten,
  !!                        syntax with equal sign is always on.
  !!  COPYRIGHT
  !!    Copyright (c) 2007-2008 Wim Van Hoydonck
  !!  AUTHOR
  !!    Wim Van Hoydonck
  !!  CREATION DATE
  !!    2007-10-04
  !!******
  implicit none

  private

  integer , parameter     :: sp = selected_real_kind(r=30,p=5)
  integer , parameter     :: dp = selected_real_kind(r=250,p=13)
#ifdef __INTEL_COMPILER
  integer , parameter     :: qp = selected_real_kind(r=350,p=20)
#endif

  ! lopts is incremented with every call to one of the specific routines of
  ! the generic 'set_option'. This makes it possible to pass the complete
  ! instance of option_t to the specific subroutines, instead of only 1 of them.
  integer :: lopts = 0

  ! if true, adds option-value pairs of the form: --option=value (or -o=value)
  logical             :: equal_sign_options = .false.
  integer , parameter :: len_key            = 32
  integer , parameter :: len_help_message   = 256
  ! len_value is the maximum length of the value of key-value pair
  ! at it appears inside the value_t that follows, it must be a parameter
  ! and cannot be changed with a call to set_parser_options
  integer , parameter :: len_value          = 256

  type :: key_t
    character(len=len_key)    :: long , short
  end type key_t

  type :: value_t
    logical                   :: value_logical
    real(sp)                  :: value_real_sp
    complex(sp)               :: value_complex_sp
    real(dp)                  :: value_real_dp
    complex(dp)               :: value_complex_dp
#ifdef __INTEL_COMPILER
    real(qp)                  :: value_real_qp
    complex(qp)               :: value_complex_qp
#endif
    integer                   :: value_integer
    character(len=len_value)  :: value_character
  end type value_t

  type :: option_t
    private
    type(key_t)                     :: key
    character(len=len_key)          :: value_type
    character(len=len_help_message) :: help_message
    ! value of the option supplied with a call to set_option, the setup or
    ! setup value, used to determine what kind of intrinsic type the option has
    type(value_t)                   :: setup
    ! value of the option supplied on the command line
    type(value_t)                   :: cmdline
    logical                         :: in_cmd_line
    character(len=32)               :: group_name
  end type option_t

  type :: key_value_pair_t
    character(len=len_value)  :: key , value
    logical                   :: is_valid
  end type key_value_pair_t




  interface set_option
      !!****s* src/utils/m_option_parser/set_option
      !!  NAME
      !!    set_option  --  set arguments of an option
      !!  SYNOPSIS
      !!    call set_option ( all_options , key_long , key_short , setup_value , help_message )
      !!  DESCRIPTION
      !!    Sets the long and short keys, a setup value and a help message
      !!    message for the next option. This is the generic interface to five
      !!    subroutines, that set the options for real and complex (both single
      !!    and double precision, and if supported by the compiler, quadruple
      !!    precision), integer, logical and character options.
      !!    An module internal variable (lopts) is incremented by 1 every time
      !!    this subroutine is called, and if it becomes larger than the
      !!    allocated size of the option_t derived type, a graceful exit is
      !!    made.
      !!  INPUTS
      !!    type(option_t)        :: all_options(:)
      !!    setup_value, one of:
      !!      logical               :: setup_value_logical
      !!      real(sp)              :: setup_value_real_sp
      !!      real(dp)              :: setup_value_real_dp
      !!      real(qp)              :: setup_value_real_qp
      !!      complex(sp)           :: setup_value_complex_sp
      !!      complex(dp)           :: setup_value_complex_dp
      !!      complex(qp)           :: setup_value_complex_qp
      !!      integer               :: setup_value_integer
      !!      character(len=len_value)    :: setup_value_character
      !!    character(len=*)      :: key_long , key_short , help_message
      !!  OUTPUT
      !!    type(option_t)        :: all_options(:)
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-05
      !!  SOURCE
    module procedure  set_option_logical    , &
                      set_option_real_sp    , &
                      set_option_complex_sp , &
                      set_option_real_dp    , &
                      set_option_complex_dp , &
#ifdef __INTEL_COMPILER
                      set_option_real_qp    , &
                      set_option_complex_qp , &
#endif
                      set_option_integer    , &
                      set_option_character
      !!******
  end interface set_option

  interface get_option_value
      !!****s* src/utils/m_option_parser/get_option_value
      !!  NAME
      !!    get_option_value  --  get the value of a key
      !!  SYNOPSIS
      !!    call get_option_value ( all_options , option_key , value )
      !!  DESCRIPTION
      !!    Get the value that belongs to the supplied key.
      !!    If the key (long or short) was not found in the command line,
      !!    its setup value is returned.
      !!    This is the generic interface to five subroutines that return the
      !!    real, real(dp), integer, character or logical value of an option.
      !!  INPUTS
      !!    type(option_t)     :: all_options(:)
      !!    character(len=*)   :: option_key_character
      !!  OUTPUT
      !!    value, one of:
      !!      real(sp)           :: value_real_sp
      !!      real(dp)           :: value_real_dp
      !!      real(qp)           :: value_real_qp
      !!      complex            :: value_complex
      !!      complex(dp)        :: value_complex_dp
      !!      complex(qp)        :: value_complex_qp
      !!      integer            :: value_integer
      !!      logical            :: value_logical
      !!      character(len=len_value) :: value_character
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
    module procedure  get_option_value_real_sp    , &
                      get_option_value_complex_sp , &
                      get_option_value_real_dp    , &
                      get_option_value_complex_dp , &
#ifdef __INTEL_COMPILER
                      get_option_value_real_qp    , &
                      get_option_value_complex_qp , &
#endif
                      get_option_value_logical    , &
                      get_option_value_integer    , &
                      get_option_value_character
      !!******
  end interface get_option_value

  ! public subroutines and derived types
  public :: set_option , get_option_value , set_parser_options , print_help_message , &
            parse_options , check_options , option_t , len_value , len_help_message

  contains

    subroutine set_parser_options ( with_equal_sign , opt2 , opt3 )
      !!****s* src/utils/m_option_parser/set_parser_options
      !!  NAME
      !!    set_parser_options  --  set some internal module/parser options
      !!  SYNOPSIS
      !!    call set_parser_options ( with_equal_sign = .true. )
      !!  DESCRIPTION
      !!    Set interal variables for the parser.
      !!    At the moment, the only option that can be set with it is
      !!    with_equal_sign, that, when set to true, enables one to set
      !!    option-value pairs of the form: --option=value, next to
      !!    --option value.
      !!  INPUTS
      !!    logical , optional :: with_equal_sign , opt2 , opt3
      !!  MODIFICATION HISTORY
      !!    2008-01-26 -- WvH : Initial version
      !!    2008-01-27 -- WvH : name chang from set_module_options to set_parser_options
      !!    2008-08-31 -- WvH : with_equal_sign does not do anything anymore
      !!  TODO
      !!    - Add options to set length of strings.
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2008-01-26
      !!  SOURCE
      implicit none

      logical , optional , intent(in) :: with_equal_sign , opt2 , opt3

      ! adds support for option-value pairs of the form: --option=value
      ! as in flightgear
      if ( present(with_equal_sign) ) then
        if (with_equal_sign) equal_sign_options = .true.
      end if

      if ( present(opt2) ) then
        ! set some internal variable

      end if

      if ( present(opt3) ) then
        ! set some internal variable

      end if
      !!******

    end subroutine set_parser_options



    subroutine check_options ( all_options , err )
      !!****s* src/utils/m_option_parser/check_options
      !!  NAME
      !!    check_options  --  check short & long keys for doubles
      !!  SYNOPSIS
      !!    call check_options ( all_options , err )
      !!  DESCRIPTION
      !!    Using the input all_options, it checks the short and long keys for
      !!    doubles. If a double is found, a warning message is send to stdout,
      !!    and control is directly returned to the invoking program unit.
      !!  INPUTS
      !!    type(option_t)      :: all_options(:)
      !!  OUTPUT
      !!    integer , optional  :: err
      !!  MODIFICATION HISTORY
      !!    2007-10-07 -- WvH : Initial version
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t) , intent(in)             :: all_options(:)
      integer        , intent(out) , optional :: err

      character(len=len_key) , dimension(size(all_options)) :: short_key_list
      character(len=len_key) , dimension(size(all_options)) :: long_key_list
      integer                                               :: all_options_size , i

      all_options_size  = size(all_options)
      if (present(err)) err = 0

      short_key_list    = all_options(:) % key % short
      long_key_list     = all_options(:) % key % long

      do i = 1 , all_options_size
        ! check the short keys for doubles
        if ( count(short_key_list == short_key_list(i)) > 1 .and.  trim(short_key_list(i)) /= "") then
          write(*,*) "WARNING : there is a short key used more than once: " , short_key_list(i)
          if (present(err)) err = err + 1
          return
        end if
        ! check the long keys for doubles
        if ( count(long_key_list == long_key_list(i)) > 1 ) then
          write(*,*) "WARNING : there is a long key used more than once: " , long_key_list(i)
          if (present(err)) err = err + 1
          return
        end if

      end do
      !!******

    end subroutine check_options



    subroutine print_help_message ( all_options , program_name , program_version , author , &
                                    copyright_years , program_description )
      !!****s* src/utils/m_option_parser/print_help_message
      !!  NAME
      !!    print_help_message  --  display help message explaining all options
      !!  SYNOPSIS
      !!    call print_help_message ( all_options , program_name , program_version , &
      !!                              author , copyright_years , program_description )
      !!  DESCRIPTION
      !!    Print a help message containing all short and long flags, the setup
      !!    values and a short explanation of every option.
      !!    If one of the optional arguments is available, print those first.
      !!    This subroutine should be called right after all options have been
      !!    attached to a variable (with get_option_value).
      !!    Minimal example:
      !!
      !!    program test_help
      !!      implicit none
      !!      use m_option_parser
      !!      type(option_t) :: program_options
      !!      logical        :: help
      !!      allocate( program_options(1) )
      !!      call set_option ( program_options , "--help" , "-h" , &
      !!                        .false. , "print help message to screen and quit" )
      !!      call get_option_value ( program_options , "-h" , help )
      !!      if ( help ) then
      !!        call print_help_message ( program_options , "My Test Program" , &
      !!                                  "1.0" ,"A.U.Thor" , "2007" )
      !!        stop
      !!      end if
      !!    end program test_help
      !!
      !!  INPUTS
      !!    type(option_t)              :: all_options(:)
      !!    character(len=*) , optional :: program_name , program_version , &
      !!                                   author , copyright_years , &
      !!                                   program_description
      !!  MODIFICATION HISTORY
      !!    2007-10-08 -- WvH : Initial version
      !!    2007-10-09 -- WvH : Header added and some cosmetic changes
      !!    2007-12-27 -- WvH : Take care of case where short key is empty
      !!                        Options are printed per group if their are any
      !!    2007-12-29 -- WvH : Simplified format statements a bit
      !!    2008-03-17 -- WvH : Usage line now contains name of binary, extracted
      !!                        with get_command_argument
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-08
      !!  SOURCE
      implicit none

      type(option_t)   , intent(in)             :: all_options(:)
      character(len=*) , intent(in) , optional  :: program_name , program_version , author , &
                                                   copyright_years , program_description

      integer                                   :: i , j , opts , idx , idx2 , groups
      integer                                   :: option_in_group(size(all_options))
      type(option_t)                            :: this_option
      character(len=32)                         :: group_names(size(all_options))
      character(len=32)                         :: this_group_name

      character(len=64)                         :: cmd_arg_name


      opts   = size(all_options)

      ! if program_name, program_version and author are present, first print
      ! those
      if (present(program_name))        write(*,*) program_name
      if (present(program_description)) write(*,*) program_description
      if (present(program_version))     write(*,*) "version " // program_version
      if (present(author) .and. .not. present(copyright_years)) then
        write(*,*) "author: " // author
      elseif (present(author) .and. present(copyright_years)) then
        write(*,*) "Copyright (c) "//copyright_years//" "//author
      end if

      ! get the name of the program
      call get_command_argument ( 0 , cmd_arg_name )

      write(*,*) ""
      write(*,*) "Usage: " , trim(cmd_arg_name) , " [options]"
      write(*,*) ""
      write(*,*) "options:"
      write(*,*) ""

      ! create array with unique group names (first is empty)
      group_names = ""
      idx2        = 1
      do i = 1 , opts
        idx = 0
        if ( trim(all_options(i) % group_name) == "" ) cycle
        this_group_name = trim(all_options(i) % group_name)
        do j = 1 , opts
          if ( trim(group_names(j)) == this_group_name) idx = 1
        end do
        if (idx == 0) then
          idx2              = idx2 + 1
          group_names(idx2) = this_group_name
        end if
      end do

      ! count number of groups
      groups = count(group_names /= "") + 1

      ! determine to which group a certain option belongs
      option_in_group = 0
      do i = 1 , opts
        if (trim(all_options(i) % group_name) == "") then
          option_in_group(i) = 1
        else
          do j = 2 , groups
            if (trim(all_options(i) % group_name) == group_names(j)) option_in_group(i) = j
          end do
        end if
      end do

      ! print options per groups as given by values of option_in_group
      do i = 1 , groups
        if (trim(group_names(i)) /= "") write(*,*) " " // trim(group_names(i)) // ":"
        do j = 1 , opts
          if (option_in_group(j) /= i) cycle

          this_option = all_options(j)

          ! check presence of short key
          if ( trim(this_option % key % short) == "" ) then
            write(*,*) "  " // trim(this_option % key % long)
          else
            write(*,*) "  " // trim(this_option % key % long) // ", " // trim(this_option % key % short)
          end if
          write(*,*) "    " // trim(this_option % help_message)

          select case ( trim(this_option % value_type) )
            case ("integer")
              write(*,*) "    ["//trim(this_option % value_type)//"]  "//"default:    " , this_option % setup % value_integer
            case ("real_sp")
              write(*,*) "    ["//trim(this_option % value_type)//"]  "//"default:    " , this_option % setup % value_real_sp
            case ("complex_sp")
              write(*,*) "    ["//trim(this_option % value_type)//"]  "//"default:    " , this_option % setup % value_complex_sp
            case ("real_dp")
              write(*,*) "    ["//trim(this_option % value_type)//"]  "//"default:    " , this_option % setup % value_real_dp
            case ("complex_dp")
              write(*,*) "    ["//trim(this_option % value_type)//"]  "//"default:    " , this_option % setup % value_complex_dp
#ifdef __INTEL_COMPILER
            case ("real_qp")
              write(*,*) "    ["//trim(this_option % value_type)//"]  "//"default:    " , this_option % setup % value_real_qp
            case ("complex_qp")
              write(*,*) "    ["//trim(this_option % value_type)//"]  "//"default:    " , this_option % setup % value_complex_qp
#endif
            case ("character")
              write(*,*) "    ["//trim(this_option % value_type)//"]  "//"default:    "//trim(this_option % setup % value_character)
            case ("logical")
              write(*,*) "    ["//trim(this_option % value_type)//"]  "//"default:    " , this_option % setup % value_logical
          end select
          write(*,*) ""

        end do
      end do
      !!******

    end subroutine print_help_message



    subroutine parse_options ( program_options )
      !!****s* src/utils/m_option_parser/parse_options
      !!  NAME
      !!    parse_options  --  parse command line and filter out valid options
      !!  SYNOPSIS
      !!    call parse_options ( program_options )
      !!  DESCRIPTION
      !!    For every key in program_options(:), add a value to program_options(:) %
      !!    cmdline % ...
      !!    This can be accomplished in two ways:
      !!    * extract values from the command line arguments,
      !!    * use the setup values if the key was not supplied on the command
      !!      line.
      !!    Once a key is identified, the next argument is put in a temporary
      !!    variable and for that one, it is checked that it is/isn't a value.
      !!    If it is not a value, it is the next key, and the setup value is
      !!    put int valid_optoins(:) % cmdline % value_{}.
      !!    If it is a value, an attempt is undertaken to convert it to the
      !!    appropriate data type for that key. If that doesn't work, the setup
      !!    is again used.
      !!  INPUTS
      !!    type(option_t)          :: program_options(:)
      !!  OUTPUT
      !!    type(option_t)          :: program_options(:)
      !!  MODIFICATION HISTORY
      !!    2007-10-06 -- WvH : Initial version
      !!    2007-10-07 -- WvH : Subroutine is finished, but could use a cleanup
      !!    2007-10-08 -- WvH : Removed second argument from the argument list
      !!    2007-12-30 -- WvH : Warn user if less options were set than the size
      !!                        of the derived type holding all data.
      !!    2008-01-25 -- WvH : Implemented --option=value syntax (as in
      !!                        flightgear), needs more testing though.
      !!    2008-02-11 -- WvH : Corrected bug with --option=value syntax.
      !!  TODO
      !!    Routine contains a lot of duplicate code, should be rewritten.
      !!  SEE ALSO
      !!    src/utils/m_option_parser/filter_options
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-06
      !!  SOURCE
      implicit none

      type(option_t)         , intent(inout)  :: program_options(:)

      integer                                 :: cmd_line_keys
      type(key_value_pair_t) , allocatable    :: key_value_pairs(:)

      integer            :: opts , cmd_length , cmd_status , cmd_arg_cnt , i , j
      integer            :: key_cnt
      integer            :: short_keys
      integer            :: sidx
      character(len=512) :: command_line
      character(len=len_value) :: value_of_keys
      character(len=len_value) , allocatable :: all_cmd_args(:)
      integer , allocatable                  :: all_cmd_args_len(:)

      character(len=len_key) , dimension(size(program_options)) :: short_key_list
      character(len=len_key) , dimension(size(program_options)) :: long_key_list
      type(key_value_pair_t) , dimension(:) , allocatable       :: tmp_key_value_pairs


      opts              = size(program_options)
      ! check that lopts and opts have the same size (equal number of command
      ! line options as size of the allocated options_t) and if not, stop
      if ( lopts < opts ) then
        write(*,*) "ERROR: less options set than the size of derived type options_t"
        write(*,*) "# options set: " , lopts
        write(*,*) "size of derived type: " , opts
        write(*,*) "please reduce the size of the derived type"
        stop
      end if

      key_cnt           = 1

      ! put all short and long keys in a list
      short_key_list    = program_options(:) % key % short(2:)
      long_key_list     = program_options(:) % key % long(3:)

      ! get command line
      call get_command ( command_line , cmd_length , cmd_status )

      ! get number of arguments
      cmd_arg_cnt = command_argument_count ()

      ! guard agains zero arguments
      if (cmd_arg_cnt > 0) then

        ! allocate space for all args
        allocate( all_cmd_args(cmd_arg_cnt) , all_cmd_args_len(cmd_arg_cnt) )
        !
        ! allocate key_w_values, this should be big enough
        ! worst case scenario:
        ! $ ./my_prog -abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ
        ! -> 1 argument, and 52 short logical keys without value
        allocate( tmp_key_value_pairs( 52*cmd_arg_cnt ) )

        tmp_key_value_pairs(:) % key    = ""
        tmp_key_value_pairs(:) % value  = ""


        ! get all arguments and their length
        do i = 1 , cmd_arg_cnt

          call get_command_argument ( i , all_cmd_args(i) , all_cmd_args_len(i) , cmd_status )

        end do

        ! classification of keys
        i = 1
        do

          if ( i > cmd_arg_cnt ) exit

          !========================!
          ! long key without value !
          !========================!

          if ( is_long_key( trim(all_cmd_args(i)) ) ) then
            !print *, "lk : " , is_long_key( trim(all_cmd_args(i)) )
            tmp_key_value_pairs(key_cnt) % key      = "--" // all_cmd_args(i)(3:all_cmd_args_len(i))
            ! check next cmd line arg a value that might belong to the current key
            if ( i < cmd_arg_cnt ) then
              if ( is_value( trim(all_cmd_args(i+1)) ) ) then
                tmp_key_value_pairs(key_cnt) % value    = trim( all_cmd_args(i+1)(1:all_cmd_args_len(i+1)) )
                i = i + 2
              else
                tmp_key_value_pairs(key_cnt) % value    = ""
                i = i + 1
              end if
              key_cnt                                 = key_cnt + 1
              cycle
            else
              exit
            end if
          endif

          !=====================!
          ! long key with value !
          !=====================!

          if ( is_long_key_with_value( trim(all_cmd_args(i)) ) ) then
            !print *, "lkwv : " , is_long_key_with_value( trim(all_cmd_args(i)) )
            sidx = index( trim(all_cmd_args(i)) , "=" )

            tmp_key_value_pairs(key_cnt) % key      = "--" // all_cmd_args(i)(3:sidx-1)
            tmp_key_value_pairs(key_cnt) % value    = trim( all_cmd_args(i)(sidx+1:all_cmd_args_len(i)) )
            key_cnt                                 = key_cnt + 1
            i = i + 1
            cycle
          endif

          !================================!
          ! single short key without value !
          !================================!

          if ( is_single_short_key( trim(all_cmd_args(i)) ) ) then
            !print *, "sk : " , is_single_short_key( trim(all_cmd_args(i)) )
            tmp_key_value_pairs(key_cnt) % key      = "-" // all_cmd_args(i)(2:all_cmd_args_len(i))
            ! check next cmd line arg a value that might belong to the current key
            if ( i < cmd_arg_cnt ) then
              if ( is_value( trim(all_cmd_args(i+1)) ) ) then
                tmp_key_value_pairs(key_cnt) % value    = trim( all_cmd_args(i+1)(1:all_cmd_args_len(i+1)) )
                i = i + 2
              else
                tmp_key_value_pairs(key_cnt) % value    = ""
                i = i + 1
              end if
              key_cnt                                 = key_cnt + 1
              cycle
            else ! i == cmd_arg_cnt, no next cmd line arg
              exit
            end if
          endif

          !=============================!
          ! single short key with value !
          !=============================!

          if ( is_single_short_key_with_value( trim(all_cmd_args(i)) ) ) then
            !print *, "skwv : " , is_single_short_key_with_value( trim(all_cmd_args(i)) )
            tmp_key_value_pairs(key_cnt) % key      = "-" // all_cmd_args(i)(2:2)
            tmp_key_value_pairs(key_cnt) % value    = trim( all_cmd_args(i)(4:all_cmd_args_len(i)) )
            key_cnt                                 = key_cnt + 1
            i = i + 1
            cycle
          endif

          !===================================!
          ! multiple short keys without value !
          !===================================!

          if ( is_multi_short_key( trim(all_cmd_args(i)) ) ) then
            !print *, "msk : " , is_multi_short_key( trim(all_cmd_args(i)) )
            short_keys = all_cmd_args_len(i) - 1
            do j = 1 , short_keys
              tmp_key_value_pairs(key_cnt+j-1) % key      = "-" // all_cmd_args(i)(j+1:j+1)
            end do
            ! check next cmd line arg a value that might belong to the current key
            if ( i < cmd_arg_cnt ) then
              if ( is_value( trim(all_cmd_args(i+1)) ) ) then
                value_of_keys = trim(all_cmd_args(i+1))
                tmp_key_value_pairs(key_cnt:key_cnt+short_keys-1) % value = trim(value_of_keys)
                i = i + 2
              else
                tmp_key_value_pairs(key_cnt:key_cnt+short_keys-1) % value = ""
                i = i + 1
              end if
              key_cnt = key_cnt + short_keys
              cycle
            else ! i == cmd_arg_cnt, no next cmd line arg
              exit
            end if
          endif

          !================================!
          ! multiple short keys with value !
          !================================!

          if ( is_multi_short_key_with_value( trim(all_cmd_args(i)) ) ) then
            !print *, "mskwv : " , is_multi_short_key_with_value( trim(all_cmd_args(i)) )
            sidx = index( all_cmd_args(i) , "=" )
            short_keys = sidx - 2
            value_of_keys = trim(all_cmd_args(i)(sidx+1:all_cmd_args_len(i)))
            do j = 1 , short_keys
              tmp_key_value_pairs(key_cnt) % key      = "-" // all_cmd_args(i)(j+1:j+1)
              tmp_key_value_pairs(key_cnt) % value    = trim(value_of_keys)
              key_cnt                                 = key_cnt + 1
            end do
            i = i + 1
            cycle
          endif

          ! if all_cmd_args(i) does not fit in any of the above categories,
          ! skip to the next one (prevents infinite loops)
          i = i + 1
        end do

        ! determine amount of command line options
        do i = 1 , size(tmp_key_value_pairs)

          ! no more keys
          if ( trim(tmp_key_value_pairs(i) % key) == "" ) then
            cmd_line_keys = i - 1
            exit
          end if

        end do

        ! exact amount of keys is known by now, allocate a new structure
        ! and assign its attributes
        allocate( key_value_pairs(cmd_line_keys))

        key_value_pairs(:) % key     = tmp_key_value_pairs(1:cmd_line_keys) % key
        key_value_pairs(:) % value   = tmp_key_value_pairs(1:cmd_line_keys) % value

        !
        ! now that we know which values were present, we must try to convert the
        ! matching values to the required format.
        call filter_options ( program_options , key_value_pairs )

      else
        ! no options are supplied on the command line
        call filter_options ( program_options )

      end if

      if ( allocated(tmp_key_value_pairs) ) deallocate( tmp_key_value_pairs )
      !!******

    end subroutine parse_options

    subroutine filter_options ( program_options , cmdl_options )
      !!***is* src/utils/m_option_parser/filter_options
      !!  NAME
      !!    filter_options  --  filter out valid options from command line
      !!  SYNOPSIS
      !!    call filter_options ( program_options , cmdl_options )
      !!  DESCRIPTION
      !!    Filters out the valid keys/options from the command line and assigns
      !!    (if available) the supplied value. If no value is available, the
      !!    setup value is used, which is always .true. for a logical or
      !!    whichever value was supplied when a certain option was set (set_option).
      !!    If cmdl_options is not present, no arguments were given on the
      !!    command line and every key gets its setup value assigned.
      !!  INPUTS
      !!    type(option_t)                    :: program_options(:)
      !!    type(key_value_pair_t) , optional :: cmdl_options(:)
      !!  OUTPUT
      !!    type(option_t)                    :: program_options(:)
      !!  MODIFICATION HISTORY
      !!    2007-10-07 -- WvH : Initial version
      !!    2007-10-14 -- WvH : Gfortran complained about lines longer than 132
      !!                        characters.
      !!    2007-12-27 -- WvH : Do not rely on short keys only to find a valid key
      !!  SEE ALSO
      !!    src/utils/m_options_parser/parse_options
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t)         , intent(inout)          :: program_options(:)
      type(key_value_pair_t) , intent(in) , optional  :: cmdl_options(:)

      type(key_value_pair_t) , allocatable  :: cmdl_opts(:)
      integer                               :: i , j , cmdl_opts_len , valid_opts_len
      integer                               :: tmp_inte , stat
      real(sp)                              :: tmp_real_sp
      complex(sp)                           :: tmp_cmplx_sp
      real(dp)                              :: tmp_real_dp
      complex(dp)                           :: tmp_cmplx_dp
#ifdef __INTEL_COMPILER
      real(qp)                              :: tmp_real_qp
      complex(qp)                           :: tmp_cmplx_qp
#endif
      logical                               :: tmp_lgcl
      character(len=len_key)                :: short_key , long_key
      character(len=len_value)              :: this_key


      ! case where no command line options were supplied
      if (present(cmdl_options)) then
        cmdl_opts_len                   = size(cmdl_options)
        allocate(cmdl_opts(cmdl_opts_len))
        cmdl_opts                       = cmdl_options
      else
        allocate(cmdl_opts(1))
        cmdl_opts_len = 1
      end if

      valid_opts_len                  = size(program_options)

      program_options(:) % in_cmd_line  = .false.

      ! loop over options, and try to convert its value
      ! to whatever type it should be, if conversion fails,
      ! use the default value
      do i = 1 , valid_opts_len

        short_key       = program_options(i) % key % short
        long_key        = program_options(i) % key % long

        do j = 1 , cmdl_opts_len

          this_key        = cmdl_opts(j) % key

          if ( (trim(this_key) == trim(short_key) ) .or. (trim(this_key) == trim(long_key)) ) then

            ! if not already found, set presence flag to true
            if ( .not. program_options(i) % in_cmd_line ) then
              program_options(i) % in_cmd_line = .true.

              !
              ! parse value to program_options(i) % cmdline % value_{character,integer,real,logical,complex}
              !
              select case ( trim(program_options(i) % value_type) )
                case ("character")

                  if ( trim(cmdl_opts(j) % value) /= "" ) then
                    program_options(i) % cmdline % value_character = cmdl_opts(j) % value
                  else    ! empty/no argument
                    program_options(i) % cmdline % value_character = trim(program_options(i) % setup % value_character)
                  end if

                case ("integer")

                  if ( trim(cmdl_opts(j) % value) /= "" ) then

                    ! try to convert the character to an integer, and if it
                    ! doesn't work, give it the setup value
                    read( unit=cmdl_opts(j) % value , fmt = *, iostat = stat ) tmp_inte
                    if ( stat == 0 ) then
                      program_options(i) % cmdline % value_integer    = tmp_inte
                    else
                      program_options(i) % cmdline % value_integer    = program_options(i) % setup % value_integer
                      write(*,*) "WARNING : could not convert cmdline value from character to " , &
                                trim(program_options(i) % value_type)
                    end if

                  else      ! empty/no argument
                    program_options(i) % cmdline % value_integer = program_options(i) % setup % value_integer
                  end if

                case ("real_sp")

                  if ( trim(cmdl_opts(j) % value) /= "" ) then

                    read( unit=cmdl_opts(j) % value , fmt=*, iostat = stat ) tmp_real_sp

                    if (stat == 0) then
                      program_options(i) % cmdline % value_real_sp = tmp_real_sp
                    else
                      program_options(i) % cmdline % value_real_sp = program_options(i) % setup % value_real_sp
                      write(*,*) "WARNING : could not convert cmdline value from character to " , &
                                trim(program_options(i) % value_type)
                    end if

                  else      ! empty/no argument
                    program_options(i) % cmdline % value_real_sp = program_options(i) % setup % value_real_sp
                  end if

                case ("real_dp")

                  if ( trim(cmdl_opts(j) % value) /= "" ) then

                    read( unit=cmdl_opts(j) % value , fmt=*, iostat = stat ) tmp_real_dp

                    if (stat == 0) then
                      program_options(i) % cmdline % value_real_dp = tmp_real_dp
                    else
                      program_options(i) % cmdline % value_real_dp = program_options(i) % setup % value_real_dp
                      write(*,*) "WARNING : could not convert cmdline value from character to " , &
                                trim(program_options(i) % value_type)
                    end if

                  else      ! empty/no argument
                    program_options(i) % cmdline % value_real_dp = program_options(i) % setup % value_real_dp
                  end if

#ifdef __INTEL_COMPILER
                case ("real_qp")

                  if ( trim(cmdl_opts(j) % value) /= "" ) then

                    read( unit=cmdl_opts(j) % value , fmt=*, iostat = stat ) tmp_real_qp

                    if (stat == 0) then
                      program_options(i) % cmdline % value_real_qp = tmp_real_qp
                    else
                      program_options(i) % cmdline % value_real_qp = program_options(i) % setup % value_real_qp
                      write(*,*) "WARNING : could not convert cmdline value from character to " , &
                                trim(program_options(i) % value_type)
                    end if

                  else      ! empty/no argument
                    program_options(i) % cmdline % value_real_qp = program_options(i) % setup % value_real_qp
                  end if

#endif
                case ("complex_sp")

                  if ( trim(cmdl_opts(j) % value) /= "" ) then

                    read( unit=cmdl_opts(j) % value , fmt=*, iostat = stat ) tmp_cmplx_sp

                    if (stat == 0) then
                      program_options(i) % cmdline % value_complex_sp = tmp_cmplx_sp
                    else
                      program_options(i) % cmdline % value_complex_sp = program_options(i) % setup % value_complex_sp
                      write(*,*) "WARNING : could not convert cmdline value from character to " , &
                                trim(program_options(i) % value_type)
                    end if

                  else      ! empty/no argument
                    program_options(i) % cmdline % value_complex_sp = program_options(i) % setup % value_complex_sp
                  end if

                case ("complex_dp")

                  if ( trim(cmdl_opts(j) % value) /= "" ) then

                    read( unit=cmdl_opts(j) % value , fmt=*, iostat = stat ) tmp_cmplx_dp

                    if (stat == 0) then
                      program_options(i) % cmdline % value_complex_dp = tmp_cmplx_dp
                    else
                      program_options(i) % cmdline % value_complex_dp = program_options(i) % setup % value_complex_dp
                      write(*,*) "WARNING : could not convert cmdline value from character to " , &
                                trim(program_options(i) % value_type)
                    end if

                  else      ! empty/no argument
                    program_options(i) % cmdline % value_complex_dp = program_options(i) % setup % value_complex_dp
                  end if

#ifdef __INTEL_COMPILER
                case ("complex_qp")

                  if ( trim(cmdl_opts(j) % value) /= "" ) then

                    read( unit=cmdl_opts(j) % value , fmt=*, iostat = stat ) tmp_cmplx_qp

                    if (stat == 0) then
                      program_options(i) % cmdline % value_complex_qp = tmp_cmplx_qp
                    else
                      program_options(i) % cmdline % value_complex_qp = program_options(i) % setup % value_complex_qp
                      write(*,*) "WARNING : could not convert cmdline value from character to " , &
                                trim(program_options(i) % value_type)
                    end if

                  else      ! empty/no argument
                    program_options(i) % cmdline % value_complex_qp = program_options(i) % setup % value_complex_qp
                  end if

#endif
                case ("logical")

                  ! logical can be: 1 , 0 , .true. , .false. , T , t , F , f ,
                  ! TRUE , true , FALSE , false , ...
                  read( unit=cmdl_opts(j) % value , fmt=*, iostat = stat ) tmp_lgcl

                  if ( trim(cmdl_opts(j) % value) /= "" ) then

                    read( unit=cmdl_opts(j) % value , fmt=*, iostat = stat ) tmp_lgcl
                    if (stat == 0) then
                      program_options(i) % cmdline % value_logical = tmp_lgcl
                    else
                      program_options(i) % cmdline % value_logical = .true.
                      write(*,*) "WARNING : could not convert cmdline value from character to " , &
                                trim(program_options(i) % value_type)
                    end if

                  else      ! empty/no argument
                    program_options(i) % cmdline % value_logical = .true.
                  end if

              end select
            end if
          end if

        end do
      end do

      ! loop for values not present in command line
      do i = 1 , valid_opts_len
        if (.not. program_options(i) % in_cmd_line) then
          select case(trim(program_options(i) % value_type))
            case("real_sp")
              program_options(i) % cmdline % value_real_sp    = program_options(i) % setup % value_real_sp
            case("complex_sp")
              program_options(i) % cmdline % value_complex_sp = program_options(i) % setup % value_complex_sp
            case("real_dp")
              program_options(i) % cmdline % value_real_dp    = program_options(i) % setup % value_real_dp
            case("complex_dp")
              program_options(i) % cmdline % value_complex_dp = program_options(i) % setup % value_complex_dp
#ifdef __INTEL_COMPILER
            case("real_qp")
              program_options(i) % cmdline % value_real_qp    = program_options(i) % setup % value_real_qp
            case("complex_qp")
              program_options(i) % cmdline % value_complex_qp = program_options(i) % setup % value_complex_qp
#endif
            case("integer")
              program_options(i) % cmdline % value_integer    = program_options(i) % setup % value_integer
            case("logical")
              program_options(i) % cmdline % value_logical    = program_options(i) % setup % value_logical
            case("character")
              program_options(i) % cmdline % value_character  = program_options(i) % setup % value_character
          end select
        end if
      end do
      !!******

    end subroutine filter_options

    subroutine set_string_options ( this_option , key_long , key_short , help_message )
      !!***is* src/utils/m_option_parser/set_string_options
      !!  NAME
      !!    set_string_options  --  set some character values for an option
      !!  SYNOPSIS
      !!    call set_sting_options ( this_options , key_long , key_short , !help_message )
      !!  DESCRIPTION
      !!    Set the character variables that hold the short key, the long key
      !!    and the help message. This subroutine packs some stuff that is
      !!    common for the four set_option_{integer,real,logical,character}.
      !!    These three variables are added to the option struct 'this_option'.
      !!  INPUTS
      !!    type(option_t)               :: this_option
      !!    character(len=*)  , optional :: key_long , key_short , help_message
      !!  OUTPUT
      !!    type(option_t)               :: this_option
      !!  MODIFICATION HISTORY
      !!    2007-10-07 -- WvH : Initial version
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t)    , intent(inout)         :: this_option
      character(len=*)  , intent(in) , optional :: key_long , key_short , help_message

      ! get optional argument values
      ! long key
      if ( present(key_long) ) then
        if ( key_long(1:2) /= "--" ) stop "long key should start with '--'"
        this_option % key % long    = key_long
      end if
      ! short key
      if ( present(key_short) ) then
        ! make sure key_short is really short
        if ( len_trim(key_short) > 2 ) stop "short key is too long"
        this_option % key % short   = key_short
      end if
      if ( present( help_message ) ) then
        this_option % help_message  = help_message
      end if
      !!******

    end subroutine set_string_options



    subroutine set_option_logical ( all_options , key_long , key_short , setup_value_logical , help_message , group_name )
      !!***is* src/utils/m_option_parser/set_option_logical
      !!  NAME
      !!    set_option_logical  --  set arguments of a logical option
      !!  SYNOPSIS
      !!    call set_option_logical ( all_options , key_long , key_short , setup_value_logical , help_message )
      !!  DESCRIPTION
      !!    Sets the long and short keys, a setup logical value and a help
      !!    message for this logical option. This is an internal subroutine, use
      !!    the generic interface subroutine 'set_option' instead.
      !!  INPUTS
      !!    logical               :: setup_value_logical
      !!    character(len=*)      :: key_long , key_short , help_message
      !!  OUTPUT
      !!    type(option_t)        :: all_options
      !!  SEE ALSO
      !!    src/utils/m_option_parser/set_option
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-05
      !!  SOURCE
      implicit none

      type(option_t)    , intent(inout)         :: all_options(:)
      logical           , intent(in)            :: setup_value_logical
      character(len=*)  , intent(in)            :: key_long , key_short , help_message
      character(len=*)  , intent(in) , optional :: group_name

      ! increment lopts with 1
      lopts = lopts + 1
      if (lopts > size(all_options)) stop "ERROR : more calls to set_option than size of the derived type"

      all_options(lopts) % setup % value_logical  = setup_value_logical
      all_options(lopts) % value_type             = "logical"
      if ( present( group_name ) ) then
        all_options(lopts) % group_name = group_name
      else
        all_options(lopts) % group_name = ""
      end if

      call set_string_options ( all_options(lopts) , key_long , key_short , help_message )
      !!******

    end subroutine set_option_logical


    subroutine set_option_character ( all_options , key_long , key_short , setup_value_character , help_message , group_name )
      !!***is* src/utils/m_option_parser/set_option_logical
      !!  NAME
      !!    set_option_character  --  set arguments of a character option
      !!  SYNOPSIS
      !!    call set_option_logical ( all_options , key_long , key_short , setup_value_character , help_message )
      !!  DESCRIPTION
      !!    Sets the long and short keys, a setup character value and a help
      !!    message for this character option. This is an internal subroutine, use
      !!    the generic interface subroutine 'set_option' instead.
      !!  INPUTS
      !!    character(len=*)      :: setup_value_character
      !!    character(len=*)      :: key_long , key_short , help_message
      !!  OUTPUT
      !!    type(option_t)        :: all_options
      !!  SEE ALSO
      !!    src/utils/m_option_parser/set_option
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-05
      !!  SOURCE
      implicit none

      type(option_t)    , intent(inout)         :: all_options(:)
      character(len=*)  , intent(in)            :: setup_value_character
      character(len=*)  , intent(in)            :: key_long , key_short , help_message
      character(len=*)  , intent(in) , optional :: group_name

      ! increment lopts with 1
      lopts = lopts + 1
      if (lopts > size(all_options)) stop "ERROR : more calls to set_option than size of the derived type"

      all_options(lopts) % setup % value_character  = setup_value_character
      all_options(lopts) % value_type               = "character"
      if ( present( group_name ) ) then
        all_options(lopts) % group_name = group_name
      else
        all_options(lopts) % group_name = ""
      end if

      call set_string_options ( all_options(lopts) , key_long , key_short , help_message )
      !!******

    end subroutine set_option_character


    subroutine set_option_real_sp ( all_options , key_long , key_short , setup_value_real_sp , help_message , group_name )
      !!***is* src/utils/m_option_parser/set_option_real_sp
      !!  NAME
      !!    set_option_real_sp  --  set arguments of a real option
      !!  SYNOPSIS
      !!    call set_option_real ( all_options , key_long , key_short , setup_value_real , help_message )
      !!  DESCRIPTION
      !!    Sets the long and short keys, a setup real value and a help
      !!    message for this real option. This is an internal subroutine, use
      !!    the generic interface subroutine 'set_option' instead.
      !!  INPUTS
      !!    real(sp)              :: setup_value_real_sp
      !!    character(len=*)      :: key_long , key_short , help_message
      !!  OUTPUT
      !!    type(option_t)        :: all_options
      !!  SEE ALSO
      !!    src/utils/m_option_parser/set_option
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-05
      !!  SOURCE
      implicit none

      type(option_t)    , intent(inout)         :: all_options(:)
      real(sp)          , intent(in)            :: setup_value_real_sp
      character(len=*)  , intent(in)            :: key_long , key_short , help_message
      character(len=*)  , intent(in) , optional :: group_name

      ! increment lopts with 1
      lopts = lopts + 1
      if (lopts > size(all_options)) stop "ERROR : more calls to set_option than size of the derived type"

      all_options(lopts) % setup % value_real_sp  = setup_value_real_sp
      all_options(lopts) % value_type             = "real_sp"
      if ( present( group_name ) ) then
        all_options(lopts) % group_name = group_name
      else
        all_options(lopts) % group_name = ""
      end if

      call set_string_options ( all_options(lopts) , key_long , key_short , help_message )
      !!******

    end subroutine set_option_real_sp


    subroutine set_option_real_dp ( all_options , key_long , key_short , setup_value_real_dp , help_message , group_name )
      !!***is* src/utils/m_option_parser/set_option_real_dp
      !!  NAME
      !!    set_option_real_dp  --  set arguments of a real (double precision) option
      !!  SYNOPSIS
      !!    call set_option_real_dp ( all_options , key_long , key_short , setup_value_real_dp , help_message )
      !!  DESCRIPTION
      !!    Sets the long and short keys, a setup double precision value and a help
      !!    message for this real option. This is an internal subroutine, use
      !!    the generic interface subroutine 'set_option' instead.
      !!  INPUTS
      !!    real(dp)              :: setup_value_real_dp
      !!    character(len=*)      :: key_long , key_short , help_message
      !!  OUTPUT
      !!    type(option_t)        :: all_options
      !!  SEE ALSO
      !!    src/utils/m_option_parser/set_option
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-05
      !!  SOURCE
      implicit none

      type(option_t)    , intent(inout)         :: all_options(:)
      real(dp)          , intent(in)            :: setup_value_real_dp
      character(len=*)  , intent(in)            :: key_long , key_short , help_message
      character(len=*)  , intent(in) , optional :: group_name

      ! increment lopts with 1
      lopts = lopts + 1
      if (lopts > size(all_options)) stop "ERROR : more calls to set_option than size of the derived type"

      all_options(lopts) % setup % value_real_dp  = setup_value_real_dp
      all_options(lopts) % value_type             = "real_dp"
      if ( present( group_name ) ) then
        all_options(lopts) % group_name = group_name
      else
        all_options(lopts) % group_name = ""
      end if

      call set_string_options ( all_options(lopts) , key_long , key_short , help_message )
      !!******

    end subroutine set_option_real_dp


#ifdef __INTEL_COMPILER
    subroutine set_option_real_qp ( all_options , key_long , key_short , setup_value_real_qp , help_message , group_name )
      !!***is* src/utils/m_option_parser/set_option_real_qp
      !!  NAME
      !!    set_option_real_dp  --  set arguments of a real (quadruple precision) option
      !!  SYNOPSIS
      !!    call set_option_real_qp ( all_options , key_long , key_short , setup_value_real_qp , help_message )
      !!  DESCRIPTION
      !!    Sets the long and short keys, a setup quadruple precision value and a help
      !!    message for this real option. This is an internal subroutine, use
      !!    the generic interface subroutine 'set_option' instead.
      !!  INPUTS
      !!    real(qp)              :: setup_value_real_qp
      !!    character(len=*)      :: key_long , key_short , help_message
      !!  OUTPUT
      !!    type(option_t)        :: all_options
      !!  SEE ALSO
      !!    src/utils/m_option_parser/set_option
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2008-02-27
      !!  SOURCE
      implicit none

      type(option_t)    , intent(inout)         :: all_options(:)
      real(qp)          , intent(in)            :: setup_value_real_qp
      character(len=*)  , intent(in)            :: key_long , key_short , help_message
      character(len=*)  , intent(in) , optional :: group_name

      ! increment lopts with 1
      lopts = lopts + 1
      if (lopts > size(all_options)) stop "ERROR : more calls to set_option than size of the derived type"

      all_options(lopts) % setup % value_real_qp  = setup_value_real_qp
      all_options(lopts) % value_type             = "real_qp"
      if ( present( group_name ) ) then
        all_options(lopts) % group_name = group_name
      else
        all_options(lopts) % group_name = ""
      end if

      call set_string_options ( all_options(lopts) , key_long , key_short , help_message )
      !!******

    end subroutine set_option_real_qp
#endif


    subroutine set_option_complex_sp ( all_options , key_long , key_short , setup_value_complex_sp , help_message , group_name )
      !!***is* src/utils/m_option_parser/set_option_complex_sp
      !!  NAME
      !!    set_option_complex_sp  --  set arguments of a complex option
      !!  SYNOPSIS
      !!    call set_option_complex_sp ( all_options , key_long , key_short , setup_value_complex_sp , help_message )
      !!  DESCRIPTION
      !!    Sets the long and short keys, a setup real value and a help
      !!    message for this real option. This is an internal subroutine, use
      !!    the generic interface subroutine 'set_option' instead.
      !!  INPUTS
      !!    complex(sp)           :: setup_value_complex_sp
      !!    character(len=*)      :: key_long , key_short , help_message
      !!  OUTPUT
      !!    type(option_t)        :: all_options
      !!  SEE ALSO
      !!    src/utils/m_option_parser/set_option
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-05
      !!  SOURCE
      implicit none

      type(option_t)    , intent(inout)         :: all_options(:)
      complex(sp)       , intent(in)            :: setup_value_complex_sp
      character(len=*)  , intent(in)            :: key_long , key_short , help_message
      character(len=*)  , intent(in) , optional :: group_name


      ! increment lopts with 1
      lopts = lopts + 1
      if (lopts > size(all_options)) stop "ERROR : more calls to set_option than size of the derived type"

      all_options(lopts) % setup % value_complex_sp   = setup_value_complex_sp
      all_options(lopts) % value_type                 = "complex_sp"
      if ( present( group_name ) ) then
        all_options(lopts) % group_name = group_name
      else
        all_options(lopts) % group_name = ""
      end if

      call set_string_options ( all_options(lopts) , key_long , key_short , help_message )
      !!******

    end subroutine set_option_complex_sp


    subroutine set_option_complex_dp ( all_options , key_long , key_short , setup_value_complex_dp , help_message , group_name )
      !!***is* src/utils/m_option_parser/set_option_complex_dp
      !!  NAME
      !!    set_option_complex_dp  --  set arguments of a complex option
      !!  SYNOPSIS
      !!    call set_option_complex_dp ( all_options , key_long , key_short , setup_value_complex_dp , help_message )
      !!  DESCRIPTION
      !!    Sets the long and short keys, a setup complex value and a help
      !!    message for this complex option. This is an internal subroutine, use
      !!    the generic interface subroutine 'set_option' instead.
      !!  INPUTS
      !!    complex(dp)           :: setup_value_complex_dp
      !!    character(len=*)      :: key_long , key_short , help_message
      !!  OUTPUT
      !!    type(option_t)        :: all_options
      !!  SEE ALSO
      !!    src/utils/m_option_parser/set_option
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-05
      !!  SOURCE
      implicit none

      type(option_t)    , intent(inout)         :: all_options(:)
      complex(dp)       , intent(in)            :: setup_value_complex_dp
      character(len=*)  , intent(in)            :: key_long , key_short , help_message
      character(len=*)  , intent(in) , optional :: group_name


      ! increment lopts with 1
      lopts = lopts + 1
      if (lopts > size(all_options)) stop "ERROR : more calls to set_option than size of the derived type"

      all_options(lopts) % setup % value_complex_dp   = setup_value_complex_dp
      all_options(lopts) % value_type                 = "complex_dp"
      if ( present( group_name ) ) then
        all_options(lopts) % group_name = group_name
      else
        all_options(lopts) % group_name = ""
      end if

      call set_string_options ( all_options(lopts) , key_long , key_short , help_message )
      !!******

    end subroutine set_option_complex_dp


#ifdef __INTEL_COMPILER
    subroutine set_option_complex_qp ( all_options , key_long , key_short , setup_value_complex_qp , help_message , group_name )
      !!***is* src/utils/m_option_parser/set_option_complex_qp
      !!  NAME
      !!    set_option_complex_qp  --  set arguments of a complex option
      !!  SYNOPSIS
      !!    call set_option_complex_qp ( all_options , key_long , key_short , setup_value_complex_qp , help_message )
      !!  DESCRIPTION
      !!    Sets the long and short keys, a setup quadruple complex value and a help
      !!    message for this option. This is an internal subroutine, use
      !!    the generic interface subroutine 'set_option' instead.
      !!  INPUTS
      !!    real                  :: setup_value_complex_qp
      !!    character(len=*)      :: key_long , key_short , help_message
      !!  OUTPUT
      !!    type(option_t)        :: all_options
      !!  SEE ALSO
      !!    src/utils/m_option_parser/set_option
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2008-02-27
      !!  SOURCE
      implicit none

      type(option_t)    , intent(inout)         :: all_options(:)
      complex(qp)       , intent(in)            :: setup_value_complex_qp
      character(len=*)  , intent(in)            :: key_long , key_short , help_message
      character(len=*)  , intent(in) , optional :: group_name


      ! increment lopts with 1
      lopts = lopts + 1
      if (lopts > size(all_options)) stop "ERROR : more calls to set_option than size of the derived type"

      all_options(lopts) % setup % value_complex_qp   = setup_value_complex_qp
      all_options(lopts) % value_type                 = "complex_qp"
      if ( present( group_name ) ) then
        all_options(lopts) % group_name = group_name
      else
        all_options(lopts) % group_name = ""
      end if

      call set_string_options ( all_options(lopts) , key_long , key_short , help_message )
      !!******

    end subroutine set_option_complex_qp
#endif


    subroutine set_option_integer ( all_options , key_long , key_short , setup_value_integer , help_message , group_name )
      !!***is* src/utils/m_option_parser/set_option_integer
      !!  NAME
      !!    set_option_integer  --  set arguments of a integer option
      !!  SYNOPSIS
      !!    call set_option_integer ( all_options , key_long , key_short , setup_value_integer , help_message )
      !!  DESCRIPTION
      !!    Sets the long and short keys, a setup integer value and a help
      !!    message for this integer option. This is an internal subroutine, use
      !!    the generic interface subroutine 'set_option' instead.
      !!  INPUTS
      !!    character(len=*)      :: setup_value_integer
      !!    character(len=*)      :: key_long , key_short , help_message
      !!  OUTPUT
      !!    type(option_t)        :: all_options
      !!  SEE ALSO
      !!    src/utils/m_option_parser/set_option
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    1007-10-05
      !!  SOURCE
      implicit none

      type(option_t)    , intent(inout)         :: all_options(:)
      integer           , intent(in)            :: setup_value_integer
      character(len=*)  , intent(in)            :: key_long , key_short , help_message
      character(len=*)  , intent(in) , optional :: group_name


      ! increment lopts with 1
      lopts = lopts + 1
      if (lopts > size(all_options)) stop "ERROR : more calls to set_option than size of the derived type"

      all_options(lopts) % setup % value_integer    = setup_value_integer
      all_options(lopts) % value_type               = "integer"
      if ( present( group_name ) ) then
        all_options(lopts) % group_name = group_name
      else
        all_options(lopts) % group_name = ""
      end if

      call set_string_options ( all_options(lopts) , key_long , key_short , help_message )
      !!******

    end subroutine set_option_integer


    subroutine get_option_value_real_sp ( all_options , option_key , value_real_sp , in_cmd_line )
      !!***is* src/utils/m_option_parser/get_option_value_real_sp
      !!  NAME
      !!    get_option_value_real_sp  --  get the real_sp value of a key
      !!  SYNOPSIS
      !!    call get_option_value_real_sp ( all_options , option_key , value_real_sp , in_cmd_line )
      !!  DESCRIPTION
      !!    Get the real value that belongs to the supplied key.
      !!    This is an internal subroutine, use the generic interface
      !!    subroutine 'get_option_value' instead.
      !!  INPUTS
      !!    type(option_t)     :: all_options(:)
      !!    character(len=*)   :: option_key
      !!  OUTPUT
      !!    real(sp)           :: value_real_sp
      !!    logical , optional :: in_cmd_line
      !!  SEE ALSO
      !!    src/utils/m_option_parser/get_option_value
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t)     , intent(in)   :: all_options(:)
      character(len=*)   , intent(in)   :: option_key
      real(sp)           , intent(out)  :: value_real_sp
      logical , optional , intent(out)  :: in_cmd_line

      integer                           :: err , option_nr

      call check_this_option ( all_options , option_key , err , option_nr )

      if ( present(in_cmd_line) ) then
        in_cmd_line = .false.
        if ( all_options(option_nr) % in_cmd_line ) in_cmd_line = .true.
      end if
      ! we are good
      if (err == 0) then
        value_real_sp = all_options(option_nr) % cmdline % value_real_sp
      else  ! no we're not
        value_real_sp = all_options(option_nr) % setup % value_real_sp
      end if
      !!******

    end subroutine get_option_value_real_sp


    subroutine get_option_value_real_dp ( all_options , option_key , value_real_dp , in_cmd_line )
      !!***is* src/utils/m_option_parser/get_option_value_real_dp
      !!  NAME
      !!    get_option_value_real_dp  --  get the real value of a key
      !!  SYNOPSIS
      !!    call get_option_value_real_dp ( all_options , option_key , value_real_dp , in_cmd_line )
      !!  DESCRIPTION
      !!    Get the double precision value that belongs to the supplied key.
      !!    This is an internal subroutine, use the generic interface
      !!    subroutine 'get_option_value' instead.
      !!  INPUTS
      !!    type(option_t)     :: all_options(:)
      !!    character(len=*)   :: option_key
      !!  OUTPUT
      !!    real(dp)           :: value_real_dp
      !!    logical , optional :: in_cmd_line
      !!  SEE ALSO
      !!    src/utils/m_option_parser/get_option_value
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t)     , intent(in)   :: all_options(:)
      character(len=*)   , intent(in)   :: option_key
      real(dp)           , intent(out)  :: value_real_dp
      logical , optional , intent(out)  :: in_cmd_line

      integer                           :: err , option_nr

      call check_this_option ( all_options , option_key , err , option_nr )

      if ( present(in_cmd_line) ) then
        in_cmd_line = .false.
        if ( all_options(option_nr) % in_cmd_line ) in_cmd_line = .true.
      end if
      ! we are good
      if (err == 0) then
        value_real_dp = all_options(option_nr) % cmdline % value_real_dp
      else  ! no we're not
        value_real_dp = all_options(option_nr) % setup % value_real_dp
      end if
      !!******

    end subroutine get_option_value_real_dp


#ifdef __INTEL_COMPILER
    subroutine get_option_value_real_qp ( all_options , option_key , value_real_qp , in_cmd_line )
      !!***is* src/utils/m_option_parser/get_option_value_real_qp
      !!  NAME
      !!    get_option_value_real_qp  --  get the real value of a key
      !!  SYNOPSIS
      !!    call get_option_value_real_qp ( all_options , option_key , value_real_qp , in_cmd_line )
      !!  DESCRIPTION
      !!    Get the quadruple precision value that belongs to the supplied key.
      !!    This is an internal subroutine, use the generic interface
      !!    subroutine 'get_option_value' instead.
      !!  INPUTS
      !!    type(option_t)     :: all_options(:)
      !!    character(len=*)   :: option_key
      !!  OUTPUT
      !!    real(qp)           :: value_real_qp
      !!    logical , optional :: in_cmd_line
      !!  SEE ALSO
      !!    src/utils/m_option_parser/get_option_value
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t)     , intent(in)   :: all_options(:)
      character(len=*)   , intent(in)   :: option_key
      real(qp)           , intent(out)  :: value_real_qp
      logical , optional , intent(out)  :: in_cmd_line

      integer                           :: err , option_nr

      call check_this_option ( all_options , option_key , err , option_nr )

      if ( present(in_cmd_line) ) then
        in_cmd_line = .false.
        if ( all_options(option_nr) % in_cmd_line ) in_cmd_line = .true.
      end if
      ! we are good
      if (err == 0) then
        value_real_qp = all_options(option_nr) % cmdline % value_real_qp
      else  ! no we're not
        value_real_qp = all_options(option_nr) % setup % value_real_qp
      end if
      !!******

    end subroutine get_option_value_real_qp
#endif


    subroutine get_option_value_complex_sp ( all_options , option_key , value_complex_sp , in_cmd_line )
      !!***is* src/utils/m_option_parser/get_option_value_complex_sp
      !!  NAME
      !!    get_option_value_complex_sp  --  get the complex value of a key
      !!  SYNOPSIS
      !!    call get_option_value_complex_sp ( all_options , option_key , value_complex_sp , in_cmd_line )
      !!  DESCRIPTION
      !!    Get the complex value that belongs to the supplied key.
      !!    This is an internal subroutine, use the generic interface
      !!    subroutine 'get_option_value' instead.
      !!  INPUTS
      !!    type(option_t)     :: all_options(:)
      !!    character(len=*)   :: option_key
      !!  OUTPUT
      !!    complex(sp)        :: value_complex_sp
      !!    logical , optional :: in_cmd_line
      !!  SEE ALSO
      !!    src/utils/m_option_parser/get_option_value
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t)     , intent(in)   :: all_options(:)
      character(len=*)   , intent(in)   :: option_key
      complex(sp)        , intent(out)  :: value_complex_sp
      logical , optional , intent(out)  :: in_cmd_line

      integer                           :: err , option_nr

      call check_this_option ( all_options , option_key , err , option_nr )

      if ( present(in_cmd_line) ) then
        in_cmd_line = .false.
        if ( all_options(option_nr) % in_cmd_line ) in_cmd_line = .true.
      end if
      ! we are good
      if (err == 0) then
        value_complex_sp = all_options(option_nr) % cmdline % value_complex_sp
      else  ! no we're not
        value_complex_sp = all_options(option_nr) % setup % value_complex_sp
      end if
      !!******

    end subroutine get_option_value_complex_sp


    subroutine get_option_value_complex_dp ( all_options , option_key , value_complex_dp , in_cmd_line )
      !!***is* src/utils/m_option_parser/get_option_value_complex_dp
      !!  NAME
      !!    get_option_value_complex_dp  --  get the complex value of a key
      !!  SYNOPSIS
      !!    call get_option_value_complex_dp ( all_options , option_key , value_complex_dp , in_cmd_line )
      !!  DESCRIPTION
      !!    Get the complex value that belongs to the supplied key.
      !!    This is an internal subroutine, use the generic interface
      !!    subroutine 'get_option_value' instead.
      !!  INPUTS
      !!    type(option_t)     :: all_options(:)
      !!    character(len=*)   :: option_key
      !!  OUTPUT
      !!    complex(dp)        :: value_complex_dp
      !!    logical , optional :: in_cmd_line
      !!  SEE ALSO
      !!    src/utils/m_option_parser/get_option_value
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t)     , intent(in)   :: all_options(:)
      character(len=*)   , intent(in)   :: option_key
      complex(dp)        , intent(out)  :: value_complex_dp
      logical , optional , intent(out)  :: in_cmd_line

      integer                           :: err , option_nr

      call check_this_option ( all_options , option_key , err , option_nr )

      if ( present(in_cmd_line) ) then
        in_cmd_line = .false.
        if ( all_options(option_nr) % in_cmd_line ) in_cmd_line = .true.
      end if
      ! we are good
      if (err == 0) then
        value_complex_dp = all_options(option_nr) % cmdline % value_complex_dp
      else  ! no we're not
        value_complex_dp = all_options(option_nr) % setup % value_complex_dp
      end if
      !!******

    end subroutine get_option_value_complex_dp


#ifdef __INTEL_COMPILER
    subroutine get_option_value_complex_qp ( all_options , option_key , value_complex_qp , in_cmd_line )
      !!***is* src/utils/m_option_parser/get_option_value_complex_qp
      !!  NAME
      !!    get_option_value_complex_qp  --  get the quadruple precision complex value of a key
      !!  SYNOPSIS
      !!    call get_option_value_complex_qp ( all_options , option_key , value_complex_qp , in_cmd_line )
      !!  DESCRIPTION
      !!    Get the complex value that belongs to the supplied key.
      !!    This is an internal subroutine, use the generic interface
      !!    subroutine 'get_option_value' instead.
      !!  INPUTS
      !!    type(option_t)     :: all_options(:)
      !!    character(len=*)   :: option_key
      !!  OUTPUT
      !!    complex(qp)        :: value_complex_qp
      !!    logical , optional :: in_cmd_line
      !!  SEE ALSO
      !!    src/utils/m_option_parser/get_option_value
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t)     , intent(in)   :: all_options(:)
      character(len=*)   , intent(in)   :: option_key
      complex(qp)        , intent(out)  :: value_complex_qp
      logical , optional , intent(out)  :: in_cmd_line

      integer                           :: err , option_nr

      call check_this_option ( all_options , option_key , err , option_nr )

      if ( present(in_cmd_line) ) then
        in_cmd_line = .false.
        if ( all_options(option_nr) % in_cmd_line ) in_cmd_line = .true.
      end if
      ! we are good
      if (err == 0) then
        value_complex_qp = all_options(option_nr) % cmdline % value_complex_qp
      else  ! no we're not
        value_complex_qp = all_options(option_nr) % setup % value_complex_qp
      end if
      !!******

    end subroutine get_option_value_complex_qp
#endif


    subroutine get_option_value_integer ( all_options , option_key , value_integer , in_cmd_line )
      !!***is* src/utils/m_option_parser/get_option_value_integer
      !!  NAME
      !!    get_option_value_integer  --  get the integer value of a key
      !!  SYNOPSIS
      !!    call get_option_value_integer ( all_options , option_key , value_integer , in_cmd_line )
      !!  DESCRIPTION
      !!    Get the integer value that belongs to the supplied key.
      !!    This is an internal subroutine, use the generic interface
      !!    subroutine 'get_option_value' instead.
      !!  INPUTS
      !!    type(option_t)     :: all_options(:)
      !!    character(len=*)   :: option_key
      !!  OUTPUT
      !!    integer            :: value_integer
      !!    logical , optional :: in_cmd_line
      !!  SEE ALSO
      !!    src/utils/m_option_parser/get_option_value
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t)     , intent(in)   :: all_options(:)
      character(len=*)   , intent(in)   :: option_key
      integer            , intent(out)  :: value_integer
      logical , optional , intent(out)  :: in_cmd_line

      integer                           :: err , option_nr

      call check_this_option ( all_options , option_key , err , option_nr )

      if ( present(in_cmd_line) ) then
        in_cmd_line = .false.
        if ( all_options(option_nr) % in_cmd_line ) in_cmd_line = .true.
      end if
      ! we are good
      if (err == 0) then
        value_integer = all_options(option_nr) % cmdline % value_integer
      else
        value_integer = all_options(option_nr) % setup % value_integer
      end if
      !!******

    end subroutine get_option_value_integer


    subroutine get_option_value_logical ( all_options , option_key , value_logical , in_cmd_line )
      !!***is* src/utils/m_option_parser/get_option_value_logical
      !!  NAME
      !!    get_option_value_logical  --  get the logical value of a key
      !!  SYNOPSIS
      !!    call get_option_value_logical ( all_options , option_key , value_logical , in_cmd_line )
      !!  DESCRIPTION
      !!    Get the logical value that belongs to the supplied key.
      !!    This is an internal subroutine, use the generic interface
      !!    subroutine 'get_option_value' instead.
      !!  INPUTS
      !!    type(option_t)     :: all_options(:)
      !!    character(len=*)   :: option_key
      !!  OUTPUT
      !!    logical            :: value_logical
      !!    logical , optional :: in_cmd_line
      !!  SEE ALSO
      !!    src/utils/m_option_parser/get_option_value
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t)     , intent(in)   :: all_options(:)
      character(len=*)   , intent(in)   :: option_key
      logical            , intent(out)  :: value_logical
      logical , optional , intent(out)  :: in_cmd_line

      integer                           :: err , option_nr

      call check_this_option ( all_options , option_key , err , option_nr )

      if ( present(in_cmd_line) ) then
        in_cmd_line = .false.
        if ( all_options(option_nr) % in_cmd_line ) in_cmd_line = .true.
      end if
      ! we are good
      if (err == 0) then
        value_logical = all_options(option_nr) % cmdline % value_logical
      else
        value_logical = all_options(option_nr) % setup % value_logical
      end if
      !!******

    end subroutine get_option_value_logical


    subroutine get_option_value_character ( all_options , option_key , value_character , in_cmd_line )
      !!***is* src/utils/m_option_parser/get_option_value_character
      !!  NAME
      !!    get_option_value_character  --  get the character value of a key
      !!  SYNOPSIS
      !!    call get_option_value_character ( all_options , option_key , value_character , in_cmd_line )
      !!  DESCRIPTION
      !!    Get the character value that belongs to the supplied key.
      !!    This is an internal subroutine, use the generic interface
      !!    subroutine 'get_option_value' instead.
      !!  INPUTS
      !!    type(option_t)        :: all_options(:)
      !!    character(len=*)      :: option_key
      !!  OUTPUT
      !!    character(len=len_value)    :: value_character
      !!    logical , optional :: in_cmd_line
      !!  SEE ALSO
      !!    src/utils/m_option_parser/get_option_value
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t)     , intent(in)   :: all_options(:)
      character(len=*)   , intent(in)   :: option_key
      character(len=len_value) , intent(out)  :: value_character
      logical , optional , intent(out)  :: in_cmd_line

      integer                           :: err , option_nr

      call check_this_option ( all_options , option_key , err , option_nr )

      if ( present(in_cmd_line) ) then
        in_cmd_line = .false.
        if ( all_options(option_nr) % in_cmd_line ) in_cmd_line = .true.
      end if
      ! we are good
      if (err == 0) then
        value_character = all_options(option_nr) % cmdline % value_character
      else
        value_character = all_options(option_nr) % setup % value_character
      end if
      !!******

    end subroutine get_option_value_character


    subroutine check_this_option ( all_options , this_option , err , idx )
      !!***is* src/utils/m_option_parser/check_this_option
      !!  NAME
      !!    check_this_option  --  check supplied key agains all valid keys
      !!  SYNOPSIS
      !!    call check_this_option ( all_options , this_option , err , idx )
      !!  DESCRIPTION
      !!    This subroutine checks the supplied option 'this_option' agains all
      !!    valid options.
      !!  INPUTS
      !!    type(option_t)      :: all_options(:)
      !!    character(len=*)    :: this_option
      !!  OUTPUT
      !!    integer             :: err , idx
      !!  MODIFICATION HISTORY
      !!    2007-10-07 -- WvH : Initial version
      !!  AUTHOR
      !!    Wim Van Hoydonck
      !!  CREATION DATE
      !!    2007-10-07
      !!  SOURCE
      implicit none

      type(option_t)   , intent(in)   :: all_options(:)
      character(len=*) , intent(in)   :: this_option
      integer          , intent(out)  :: err , idx

      integer                         :: i , all_options_size
      character(len=len_key) , dimension(size(all_options))  :: short_key_list
      character(len=len_key) , dimension(size(all_options))  :: long_key_list

      err = 0
      all_options_size  = size(all_options)

      ! put all short and long keys in a list
      short_key_list    = all_options(:) % key % short
      long_key_list     = all_options(:) % key % long

      ! check that 'this_option' is a valid flag
      ! short options
      if (len(this_option) == 2) then
        if (count(short_key_list == trim(this_option)) /= 1 .and.  trim(this_option) /= "") then
          write(*,*) "WARNING : could not find supplied key " , this_option
          err = 1
        else
          do i = 1 , all_options_size
            if ( trim(all_options(i) % key % short) == trim(this_option) ) idx = i
          end do
        end if
      else
        ! check the long keys for doubles
        if (count(long_key_list == trim(this_option)) /= 1 ) then
          write(*,*) "WARNING : could not find supplied key " , this_option
          err = 1
        else
          do i = 1 , all_options_size
            !if ( trim(all_options(i) % key % short) == this_option ) idx = i
            if ( trim(all_options(i) % key % long) == this_option ) idx = i
          end do
        end if
      end if
      !!******

    end subroutine check_this_option

    function is_long_key ( pkey ) result ( is_key )
      ! for a key to be a single long key, the following properties
      ! should be checked:
      !   - its length should be 3 or more
      !   - the first two characters should be "--"
      !   - cannot contain an equal sign
      implicit none

      character(len=*) , intent(in) :: pkey ! potential key
      logical                       :: is_key

      integer :: len_key

      is_key = .false.
      len_key = len( pkey )

      if ( len_key < 3 ) return
      if ( scan(pkey,"=") > 0) return
      if ( pkey(1:2) == "--" ) is_key = .true.

    end function is_long_key

    function is_long_key_with_value ( pkey ) result ( is_key )
      ! for a key to be a single long key with a value,
      ! the following properties should be checked:
      !   - it should be a long key
      !   - its length should be at least 5: --?=?
      !   - it must contain at least one equal sign
      implicit none
      character(len=*) , intent(in) :: pkey ! potential key
      logical                       :: is_key

      integer :: len_key

      is_key = .false.
      len_key = len( pkey )

      if ( len_key < 5 ) return
      if ( pkey(1:2) /= "--" ) return
      if ( scan(pkey,"=") > 0) is_key = .true.

    end function is_long_key_with_value


    function is_single_short_key ( pkey ) result ( is_key )
      ! for a key to be a single short key, the following
      ! properties should be checked:
      !   - its length (including leading) should be two
      !   - the first character should be a dash
      !   - the second character should be one of [a-zA-Z]
      implicit none

      character(len=*) , intent(in) :: pkey ! potential key
      logical                       :: is_key

      integer :: len_key , ascii_val

      is_key  = .false.
      len_key = len( pkey )

      if ( len_key /= 2 ) return
      if ( pkey(1:1) /= "-" ) return
      ascii_val = iachar( pkey(2:2) )
      if ( (ascii_val >= 65 .and. ascii_val <= 90) .or. (ascii_val >= 97 .and. ascii_val <= 122) ) then
        is_key = .true.
      end if

    end function is_single_short_key

    function is_single_short_key_with_value ( pkey ) result ( is_key )
      ! for a key to be a single short key with value,
      ! the following properties should be checked:
      !   - its length should be at least four -x=x
      !   - the first character should be a dash,
      !   - the third character should be an equal sign
      !   - the second character should be one of [a-zA-Z]
      implicit none

      character(len=*) , intent(in) :: pkey ! potential key
      logical                       :: is_key

      integer :: len_key , ascii_val

      is_key  = .false.
      len_key = len( pkey )
      if ( len_key < 4 ) return
      if ( pkey(1:1) /= "-" .or. pkey(3:3) /= "=") return
      ascii_val = iachar( pkey(2:2) )
      if ( (ascii_val >= 65 .and. ascii_val <= 90) .or. (ascii_val >= 97 .and. ascii_val <= 122) ) then
        is_key = .true.
      end if

    end function is_single_short_key_with_value

    function is_multi_short_key ( pkey ) result ( is_key )
      ! for a key to be a single short key,
      ! the following properties should be checked:
      !   - its length should be at least three
      !   - the first character must be a dash
      !   - all characters after the dash should be one of [a-zA-Z]
      implicit none

      character(len=*) , intent(in) :: pkey ! potential key
      logical                       :: is_key

      integer :: len_key , i , ascii_val , ascii_vals

      is_key  = .false.
      len_key = len( pkey )
      ascii_vals = 0

      if ( len_key < 3 ) return
      if ( pkey(1:1) /= "-" ) return
      do i = 2 , len_key
        ascii_val = iachar( pkey(i:i) )
        if ( (ascii_val >= 65 .and. ascii_val <= 90) .or. (ascii_val >= 97 .and. ascii_val <= 122) ) then
          ascii_vals = ascii_vals + 1
        end if
      end do
      if ( ascii_vals == len_key - 1 ) then
        is_key = .true.
      end if

    end function is_multi_short_key

    function is_multi_short_key_with_value ( pkey ) result ( is_key )
      ! for a key to be a single short key,
      ! the following properties should be checked:
      !   - its length should be at least five: -xy=a
      !   - the first character should be a dash
      !   - their should be 1 equal sign in the range fourth -> next-to-last
      !   - all characters between the dash and the equal sign should be
      !     one of [a-zA-Z]
      implicit none

      character(len=*) , intent(in) :: pkey ! potential key
      logical                       :: is_key

      integer :: i , len_key , equal_idx , ascii_vals , ascii_val

      is_key  = .false.
      len_key = len( pkey )

      if ( len_key < 5 ) return
      if ( pkey(1:1) /= "-" ) return
      equal_idx = scan( pkey(1:len_key-1) , "=" )
      if ( equal_idx > 0 ) then
        ascii_vals = 0
        do i = 2 , equal_idx - 1
          ascii_val = iachar( pkey(i:i) )
          if ( (ascii_val >= 65 .and. ascii_val <= 90) .or. (ascii_val >= 97 .and. ascii_val <= 122) ) then
            ascii_vals = ascii_vals + 1
          end if
        end do
        if ( ascii_vals == equal_idx - 2) then
          is_key = .true.
        end if
      end if

    end function is_multi_short_key_with_value

    function is_value( pvalue ) result ( is_val )
      ! for an argument to be a value,
      ! the following properties should be checked:
      !   - if it starts with a minus sign, the first following character
      !     should be numeric, the rest then doesn't matter
      !   - if it does not start with a minus signs, it does not matter
      !     what the following characters are
      implicit none

      character(len=*) , intent(in) :: pvalue ! potential value
      logical                       :: is_val

      integer :: len_value , ascii_val

      is_val    = .false.
      len_value = len( pvalue )

      if ( pvalue(1:1) == "-" ) then
        ascii_val = iachar( pvalue(2:2) )
        !print *, ascii_val
        if ( ascii_val >= 48 .and. ascii_val <= 57 .or. ascii_val == 46 ) then
          is_val = .true.
        end if
      else
        is_val = .true.
      end if

    end function is_value


end module m_option_parser

